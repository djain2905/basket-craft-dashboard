import os
import streamlit as st
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

st.title("BasketCraft Dashboard")

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="BASKET_CRAFT",
        schema="RAW",
    )

@st.cache_data(ttl=600)
def get_dim_products_count():
    conn = get_connection()
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute("SELECT COUNT(*) AS cnt FROM BASKET_CRAFT.ANALYTICS.DIM_PRODUCTS")
    return cur.fetchone()["CNT"]

@st.cache_data(ttl=600)
def get_headline_metrics():
    conn = get_connection()
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute("""
        WITH monthly AS (
            SELECT
                DATE_TRUNC('month', TO_DATE(TO_TIMESTAMP(CREATED_AT, 6))) AS month,
                SUM(PRICE_USD)                                            AS revenue,
                COUNT(DISTINCT ORDER_ID)                                  AS orders,
                SUM(ITEMS_PURCHASED)                                      AS items_sold
            FROM ORDERS
            GROUP BY 1
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (ORDER BY month DESC) AS rn
            FROM monthly
        )
        SELECT
            TO_CHAR(month, 'YYYY-MM')                     AS month,
            ROUND(revenue, 2)                             AS revenue,
            orders,
            ROUND(revenue / NULLIF(orders, 0), 2)         AS aov,
            items_sold
        FROM ranked
        WHERE rn <= 2
        ORDER BY rn
    """)
    rows = cur.fetchall()
    cur.close()
    return rows  # rows[0] = current month, rows[1] = prior month

@st.cache_data(ttl=600)
def get_monthly_revenue():
    conn = get_connection()
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute("""
        SELECT
            TO_DATE(DATE_TRUNC('month', TO_DATE(TO_TIMESTAMP(CREATED_AT, 6)))) AS month,
            ROUND(SUM(PRICE_USD), 2) AS revenue
        FROM ORDERS
        GROUP BY 1
        ORDER BY 1
    """)
    rows = cur.fetchall()
    cur.close()
    df = pd.DataFrame([{"month": r["MONTH"], "revenue": r["REVENUE"]} for r in rows])
    df["month"] = pd.to_datetime(df["month"].astype(str))
    return df

@st.cache_data(ttl=600)
def get_copurchase_matrix():
    conn = get_connection()
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute("""
        SELECT
            p_anchor.PRODUCT_NAME          AS anchor_product,
            p_other.PRODUCT_NAME           AS paired_product,
            COUNT(DISTINCT a.ORDER_ID)     AS co_purchase_count
        FROM ORDER_ITEMS a
        JOIN ORDER_ITEMS b     ON b.ORDER_ID = a.ORDER_ID AND b.PRODUCT_ID != a.PRODUCT_ID
        JOIN PRODUCTS p_anchor ON p_anchor.PRODUCT_ID = a.PRODUCT_ID
        JOIN PRODUCTS p_other  ON p_other.PRODUCT_ID  = b.PRODUCT_ID
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame([{
        "anchor_product": r["ANCHOR_PRODUCT"],
        "paired_product": r["PAIRED_PRODUCT"],
        "co_purchase_count": r["CO_PURCHASE_COUNT"],
    } for r in rows])

@st.cache_data(ttl=600)
def get_product_revenue_by_month():
    conn = get_connection()
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute("""
        SELECT
            TO_DATE(DATE_TRUNC('month', TO_DATE(TO_TIMESTAMP(o.CREATED_AT, 6)))) AS month,
            p.PRODUCT_NAME,
            ROUND(SUM(oi.PRICE_USD), 2) AS revenue
        FROM ORDER_ITEMS oi
        JOIN ORDERS o  ON o.ORDER_ID  = oi.ORDER_ID
        JOIN PRODUCTS p ON p.PRODUCT_ID = oi.PRODUCT_ID
        GROUP BY 1, 2
        ORDER BY 1
    """)
    rows = cur.fetchall()
    cur.close()
    df = pd.DataFrame([{"month": r["MONTH"], "product": r["PRODUCT_NAME"], "revenue": r["REVENUE"]} for r in rows])
    df["month"] = pd.to_datetime(df["month"].astype(str))
    return df

def delta_str(current, prior, prefix="", fmt=","):
    if prior == 0:
        return "N/A"
    pct = (current - prior) / prior * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}% vs {prior:{fmt}}"

# ── Headline Metrics ──────────────────────────────────────────────────────────
st.subheader("Headline Metrics")

try:
    rows = get_headline_metrics()
    cur_m, pri_m = rows[0], rows[1]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Total Revenue",
            value=f"${cur_m['REVENUE']:,.2f}",
            delta=delta_str(cur_m["REVENUE"], pri_m["REVENUE"], prefix="$", fmt=",.2f"),
        )
    with col2:
        st.metric(
            label="Total Orders",
            value=f"{cur_m['ORDERS']:,}",
            delta=delta_str(cur_m["ORDERS"], pri_m["ORDERS"]),
        )
    with col3:
        st.metric(
            label="Avg Order Value",
            value=f"${cur_m['AOV']:,.2f}",
            delta=delta_str(cur_m["AOV"], pri_m["AOV"], prefix="$", fmt=",.2f"),
        )
    with col4:
        st.metric(
            label="Items Sold",
            value=f"{cur_m['ITEMS_SOLD']:,}",
            delta=delta_str(cur_m["ITEMS_SOLD"], pri_m["ITEMS_SOLD"]),
        )
    st.caption(f"Current month: {cur_m['MONTH']} · Prior month: {pri_m['MONTH']}")

except Exception as e:
    st.error(f"Failed to load headline metrics: {e}")

# ── Shared date filter ────────────────────────────────────────────────────────
try:
    df_trend = get_monthly_revenue()
    min_date = df_trend["month"].min().date()
    max_date = df_trend["month"].max().date()

    start, end = st.slider(
        "Date range",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="MMM YYYY",
    )
except Exception as e:
    st.error(f"Failed to initialise date filter: {e}")
    st.stop()

# ── Revenue Trend ─────────────────────────────────────────────────────────────
st.subheader("Revenue Trend")

try:
    mask = (df_trend["month"].dt.date >= start) & (df_trend["month"].dt.date <= end)
    filtered_trend = df_trend[mask].set_index("month")
    st.line_chart(filtered_trend["revenue"], y_label="Revenue (USD)", x_label="Month")
    st.caption(f"Showing {len(filtered_trend)} months · ${filtered_trend['revenue'].sum():,.2f} total revenue in range")

except Exception as e:
    st.error(f"Failed to load revenue trend: {e}")

# ── Top Products by Revenue ───────────────────────────────────────────────────
st.subheader("Top Products by Revenue")

try:
    df_products = get_product_revenue_by_month()
    mask_p = (df_products["month"].dt.date >= start) & (df_products["month"].dt.date <= end)
    top_products = (
        df_products[mask_p]
        .groupby("product", as_index=False)["revenue"]
        .sum()
        .sort_values("revenue", ascending=False)
        .set_index("product")
    )
    st.bar_chart(top_products["revenue"], y_label="Revenue (USD)", x_label="Product")

except Exception as e:
    st.error(f"Failed to load top products: {e}")

# ── Bundle Finder ─────────────────────────────────────────────────────────────
st.subheader("Bundle Finder")

try:
    df_pairs = get_copurchase_matrix()
    products = sorted(df_pairs["anchor_product"].unique().tolist())

    selected = st.selectbox("Pick a product", products)

    pairs = (
        df_pairs[df_pairs["anchor_product"] == selected]
        .sort_values("co_purchase_count", ascending=False)
        [["paired_product", "co_purchase_count"]]
        .rename(columns={"paired_product": "Paired Product", "co_purchase_count": "Orders Together"})
        .reset_index(drop=True)
    )
    pairs.index += 1

    st.dataframe(pairs, use_container_width=True)

except Exception as e:
    st.error(f"Failed to load bundle finder: {e}")

# ── Smoke Test ────────────────────────────────────────────────────────────────
with st.expander("Snowflake connection smoke test"):
    try:
        count = get_dim_products_count()
        st.success(f"Connected. `DIM_PRODUCTS` has **{count:,}** rows.")
    except Exception as e:
        st.error(f"Connection failed: {e}")
