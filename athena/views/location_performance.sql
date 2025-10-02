CREATE OR REPLACE VIEW biz_insights.v_location_performance_90d AS
WITH maxd AS (SELECT max(order_date) AS d FROM biz_insights.gold_daily_sales_by_store),
-- revenue + orders (G1)
rev AS (
  SELECT
    restaurant_id,
    CAST(SUM(gross_sales_total) AS DECIMAL(14,2)) AS gross_90d,
    SUM(orders_count) AS orders_90d
  FROM biz_insights.gold_daily_sales_by_store
  WHERE order_date BETWEEN date_add('day', -89, (SELECT d FROM maxd)) AND (SELECT d FROM maxd)
  GROUP BY restaurant_id
),
-- per-store user repeat from Silver at order grain
orders AS (
  SELECT DISTINCT restaurant_id, order_id, user_id
  FROM biz_insights.silver_order_items
  WHERE order_date BETWEEN date_add('day', -89, (SELECT d FROM maxd)) AND (SELECT d FROM maxd)
),
per_user AS (
  SELECT restaurant_id, user_id, COUNT(DISTINCT order_id) AS user_orders_90d
  FROM orders GROUP BY restaurant_id, user_id
)
SELECT
  r.restaurant_id,
  r.gross_90d,
  r.orders_90d,
  CASE WHEN r.orders_90d > 0 THEN CAST(r.gross_90d / r.orders_90d AS DECIMAL(14,2)) END AS aov_90d,
  COUNT(DISTINCT p.user_id) AS customers_90d,
  CAST(AVG(CASE WHEN p.user_orders_90d >= 2 THEN 1 ELSE 0 END) AS DOUBLE) AS repeat_rate_90d
FROM rev r
LEFT JOIN per_user p ON p.restaurant_id = r.restaurant_id
GROUP BY r.restaurant_id, r.gross_90d, r.orders_90d;

