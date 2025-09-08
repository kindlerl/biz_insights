-- Set your target date (example: yesterday UTC)
WITH params AS (
  SELECT date(date_add('day', -1, current_date)) AS d
),
opts AS (
  SELECT order_id, lineitem_id,
         SUM(option_price * option_quantity) AS options_total
  FROM biz_insights.silver_order_items_options
  GROUP BY 1,2
),
enriched AS (
  SELECT i.restaurant_id, i.order_date, i.order_id, i.item_quantity, i.order_total,
         COALESCE(o.options_total, 0) AS options_total,
         (i.order_total + COALESCE(o.options_total,0)) AS line_total,
         i.is_loyalty
  FROM biz_insights.silver_order_items i
  LEFT JOIN opts o USING (order_id, lineitem_id)
  JOIN params p ON i.order_date = p.d
),
orders AS (
  SELECT restaurant_id, order_date, order_id,
         SUM(line_total) AS order_gross,
         SUM(order_total) AS order_item_sales,
         SUM(options_total) AS order_option_sales,
         SUM(item_quantity) AS order_items_count,
         CAST(MAX(CASE WHEN is_loyalty THEN 1 ELSE 0 END) AS BOOLEAN) AS is_loyalty_order
  FROM enriched
  GROUP BY restaurant_id, order_date, order_id
)
INSERT OVERWRITE biz_insights.gold_daily_sales_by_store
SELECT
  restaurant_id,
  order_date,
  SUM(order_item_sales)  AS item_sales,
  SUM(order_option_sales) AS option_sales,
  SUM(order_gross)       AS gross_sales_total,
  COUNT(DISTINCT order_id) AS orders_count,
  SUM(order_items_count) AS items_count,
  SUM(CASE WHEN is_loyalty_order THEN order_gross ELSE 0 END) AS loyalty_sales,
  SUM(CASE WHEN is_loyalty_order THEN 1 ELSE 0 END)          AS loyalty_orders,
  SUM(order_gross) - SUM(CASE WHEN is_loyalty_order THEN order_gross ELSE 0 END) AS non_loyalty_sales,
  COUNT(DISTINCT order_id) - SUM(CASE WHEN is_loyalty_order THEN 1 ELSE 0 END)   AS non_loyalty_orders
FROM orders
GROUP BY restaurant_id, order_date;

