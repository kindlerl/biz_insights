CREATE OR REPLACE VIEW biz_insights.v_sales_monthly_store_12m AS
WITH maxd AS (
  SELECT CAST(MAX(CAST(order_date AS DATE)) AS DATE) AS d
  FROM biz_insights.gold_daily_sales_by_store
),
m AS (
  SELECT
    restaurant_id,
    date_trunc('month', CAST(order_date AS DATE)) AS month,
    CAST(SUM(gross_sales_total) AS DECIMAL(14,2)) AS gross_month
  FROM biz_insights.gold_daily_sales_by_store t
  CROSS JOIN maxd
  WHERE CAST(t.order_date AS DATE)
        BETWEEN date_add('month', CAST(-11 AS BIGINT), maxd.d) AND maxd.d
  GROUP BY
    restaurant_id,
    date_trunc('month', CAST(order_date AS DATE))
)
SELECT
  restaurant_id,
  month,
  gross_month,
  (gross_month - LAG(gross_month) OVER (PARTITION BY restaurant_id ORDER BY month))
    / NULLIF(LAG(gross_month) OVER (PARTITION BY restaurant_id ORDER BY month), 0) AS mom_pct
FROM m;
