# Business Insights – Analytics Pipeline (Bronze → Silver → Gold)

End-to-end, low-cost analytics stack on AWS using **S3 + Glue + Athena + Streamlit**.  
Daily job ingests/cleans data and materializes a small set of reusable Gold facts that power 7 KPIs.

---

## Quick Facts

- **Region:** `us-east-1`  
- **Bucket:** `bus-insights-dev-us-east-1`  
- **Athena Workgroup:** `primary` (or project WG)  
- **Database (Glue/Athena):** `biz_insights`  
- **Daily Schedule:** once/day via EventBridge → Glue Workflow (3–4 nodes)

---

## Architecture (Google-Maps view)

**Sources** → SQL Server (staging & basic typing)  
**Bronze (S3/parquet)** → landed from SQL Server (via Glue job), validated in Athena  
**Silver (S3/parquet)** → type-cast, light transforms (Spark)  
**Gold (S3/parquet)** → few reusable facts (Athena CTAS or Spark), partitioned by `order_date`  
**Serving** → Athena SQL views + Streamlit

---

## Environments & Prereqs

- AWS account with access to **S3, Glue, Athena, EventBridge, IAM**
- Local Python (for Streamlit app); see `requirements.txt`
- S3 prefixes created:
  - `bronze/`, `silver/`, `gold/`, `athena/results/`, `tmp/` (optional)

---
## Set Up (once)

1. **Glue Data Catalog DB**
   ```sql
   CREATE DATABASE IF NOT EXISTS biz_insights;
Athena Workgroup Results Location
Set to s3://bus-insights-dev-us-east-1/athena/results/.

IAM (least-priv summary)

Glue Job Role: read bronze/*, write silver/* and gold/*

Your user/role: Athena query permissions + write to athena/results/*

Bronze Tables
Prefer Glue Crawler pointing at s3://bus-insights-dev-us-east-1/bronze/ → biz_insights with prefix bronze_.

Bronze → Validation (Athena)
Run from sql/bronze/validation_checks.sql (row counts, distinct keys, orphan options, parseability).
Goal: verify counts match the SQL Server seed and no orphans.

Silver
Implemented via glue/jobs/silver_build.py (Spark, Glue 4.0)

Outputs:

silver/order_items/ (casts, order_total, order_date)

silver/order_items_options/ (casts)

silver/date_dim/ (casts)

Register external tables with sql/silver/silver_tables.sql

Validate with sql/silver/validation_checks.sql (counts match Bronze; null checks).

Gold (reusable facts powering all KPIs)
G1. gold_daily_sales_by_store (partitioned by order_date)
Grain: restaurant × day. Includes items + options, orders count, loyalty split.

Create once (CTAS): sql/gold/g1_daily_sales_by_store_ctas.sql

Daily refresh (insert/overwrite partition): sql/gold/g1_daily_sales_by_store_insert_overwrite.sql

G2. gold_item_sales_by_day (partitioned by order_date)
Grain: restaurant × day × item. Drives Top Items (e.g., 30-day).

CTAS in sql/gold/g2_item_sales_by_day_ctas.sql

G3. gold_customer_facts (optional Spark job)
Grain: user_id snapshot. CLV/RFM/churn features. (Add when needed for customer KPIs.)

G4. gold_discounts_by_day (optional)
Grain: restaurant × day. Discounted vs non-discounted metrics (option_price < 0).

CTAS in sql/gold/g4_discounts_by_day_ctas.sql

Views (presentation layer): sql/gold/views.sql

v_daily_sales_by_store (adds pct columns)

v_top_items_30d (rolling window)

v_location_ranking (rank stores by revenue/AOV)

Daily Scheduling
Glue Workflow (suggested node order):

Silver job

G1 (and G2/G4 in parallel if using Glue; or run CTAS via scheduled Athena if you prefer)

(Optional) G3 customer_facts

EventBridge cron → run once/day.

3:00 AM America/Chicago:

CDT (UTC-5): cron(0 8 * * ? *)

CST (UTC-6): cron(0 9 * * ? *)

Idempotency: each Gold job overwrites only the target order_date partition.

Backfill: allow --PROCESS_DATE=YYYY-MM-DD for manual re-runs.

Streamlit
App code under streamlit_dashboard/.

Typical dependencies: streamlit, pandas, pyathena, plotly (configure in requirements.txt).

Suggested pages:

📈 Sales Trends (from v_daily_sales_by_store)

🏪 Top Locations (rank by revenue/AOV)

🍔 Top Items 30d (from v_top_items_30d)

🎟️ Loyalty Impact (loyalty vs non-loyalty)

👤 Customer Facts (from gold_customer_facts)

