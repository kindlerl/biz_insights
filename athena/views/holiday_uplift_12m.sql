CREATE OR REPLACE VIEW biz_insights.v_holiday_uplift_12m AS
WITH last AS (
  SELECT CAST(MAX(CAST(order_date AS DATE)) AS DATE) AS max_dt
  FROM biz_insights.gold_daily_sales_by_store
),
base AS (
  SELECT
    g.restaurant_id,
    CAST(g.order_date AS DATE) AS order_date,
    g.gross_sales_total,
    d.holiday_name,
    d.is_holiday,
    d.day_of_week
  FROM biz_insights.gold_daily_sales_by_store g
  JOIN biz_insights.silver_date_dim d
    ON CAST(d.date_key AS DATE) = CAST(g.order_date AS DATE)   -- align types
  CROSS JOIN last
  WHERE CAST(g.order_date AS DATE)
        BETWEEN date_add('month', CAST(-11 AS BIGINT), last.max_dt)
            AND last.max_dt
),
hol AS (
  SELECT restaurant_id, order_date, holiday_name, day_of_week
  FROM base
  WHERE is_holiday = TRUE
)
SELECT
  b.restaurant_id,
  h.holiday_name,
  CAST(SUM(b.gross_sales_total) AS DECIMAL(14,2)) AS holiday_sales,
  CAST(AVG(b2.gross_sales_total) AS DECIMAL(14,2)) AS baseline_sales,
  CAST(SUM(b.gross_sales_total) / NULLIF(AVG(b2.gross_sales_total), 0) - 1 AS DOUBLE) AS uplift_pct
FROM hol h
JOIN base b
  ON b.restaurant_id = h.restaurant_id
 AND b.order_date    = h.order_date
JOIN base b2
  ON b2.restaurant_id = h.restaurant_id
 AND b2.is_holiday    = FALSE
 AND b2.day_of_week   = h.day_of_week
GROUP BY b.restaurant_id, h.holiday_name;
