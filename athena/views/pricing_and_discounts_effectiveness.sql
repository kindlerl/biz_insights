CREATE OR REPLACE VIEW biz_insights.v_discount_effectiveness_30d AS
WITH maxd AS (SELECT max(order_date) AS d FROM biz_insights.gold_discounts_by_day)
SELECT
  restaurant_id,
  CAST(SUM(pre_discount_sales)       AS DECIMAL(14,2)) AS pre_sales_30d,
  CAST(SUM(discount_amount_abs)      AS DECIMAL(14,2)) AS discount_30d,
  CAST(SUM(net_sales_after_discounts)AS DECIMAL(14,2)) AS net_sales_30d,
  CAST(SUM(discounted_orders)        AS DOUBLE)        AS discounted_orders_30d,
  CAST(SUM(orders_count)             AS DOUBLE)        AS orders_30d,
  CASE WHEN SUM(orders_count) > 0 THEN SUM(discounted_orders) / SUM(orders_count) ELSE 0 END AS pct_orders_discounted_30d
FROM biz_insights.gold_discounts_by_day
WHERE order_date BETWEEN date_add('day', -29, (SELECT d FROM maxd)) AND (SELECT d FROM maxd)
GROUP BY restaurant_id;

