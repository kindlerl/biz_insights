CREATE OR REPLACE VIEW biz_insights.v_sales_dow_store_12m AS
WITH last AS (
  SELECT CAST(MAX(CAST(order_date AS DATE)) AS DATE) AS max_dt
  FROM biz_insights.gold_daily_sales_by_store
)
SELECT
  g.restaurant_id,
  d.day_of_week,
  CAST(AVG(g.gross_sales_total) AS DECIMAL(14,2)) AS avg_sales_dow
FROM biz_insights.gold_daily_sales_by_store g
JOIN biz_insights.silver_date_dim d
  ON CAST(g.order_date AS DATE) = CAST(d.date_key AS DATE)   -- align types
CROSS JOIN last
WHERE CAST(g.order_date AS DATE)
      BETWEEN date_add('month', CAST(-11 AS BIGINT), last.max_dt)
          AND last.max_dt
GROUP BY g.restaurant_id, d.day_of_week;
