# BasketCraft Dashboard

**Live app:** https://djain2905-basket-craft-dashboard-app-rvea2d.streamlit.app

## Overview

A Streamlit dashboard connected to the BasketCraft Snowflake data warehouse. It provides headline KPIs, a revenue trend chart, a top-products breakdown, and a bundle finder — all filterable by date range.

## Features

| Section | Description |
|---|---|
| Headline Metrics | Revenue, orders, AOV, and items sold vs. the prior month |
| Revenue Trend | Monthly revenue line chart with a drag-to-filter date slider |
| Top Products by Revenue | Bar chart of product revenue, filtered by the date slider |
| Bundle Finder | Pick a product and see what gets bought alongside it most often |

## Local setup

```bash
pip install -r requirements.txt
```

Create a `.env` file with your Snowflake credentials:

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
```

Then run:

```bash
streamlit run app.py
```

## Streamlit Cloud deployment

Secrets are configured in **App settings → Secrets** using TOML format:

```toml
SNOWFLAKE_ACCOUNT = "..."
SNOWFLAKE_USER = "..."
SNOWFLAKE_PASSWORD = "..."
```
