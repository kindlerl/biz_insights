-- partitions resolve?
SELECT *
FROM biz_insights.gold_discounts_by_day
WHERE order_date = (SELECT max(order_date) FROM biz_insights.gold_daily_sales_by_store)
ORDER BY discount_amount_abs DESC
LIMIT 20;

-- pre - discount == net ?
SELECT max(ABS(pre_discount_sales - discount_amount_abs - net_sales_after_discounts)) AS max_diff
FROM biz_insights.gold_discounts_by_day;

-- discounted_orders <= orders_count ?
SELECT min(orders_count - discounted_orders) AS min_gap
FROM biz_insights.gold_discounts_by_day;

-- same-day spot check (pick a day you know exists)
SELECT * FROM biz_insights.gold_discounts_by_day
WHERE order_date = (SELECT max(order_date) FROM biz_insights.gold_daily_sales_by_store)
ORDER BY discount_amount_abs DESC
LIMIT 20;

-- pre - discount == net ?  (should be 0.00)
SELECT max(ABS(pre_discount_sales - discount_amount_abs - net_sales_after_discounts)) AS max_diff
FROM biz_insights.gold_discounts_by_day;

-- discounted_orders <= orders_count ?  (min_gap should be >= 0)
SELECT min(orders_count - discounted_orders) AS min_gap
FROM biz_insights.gold_discounts_by_day;

-- Cross-check vs G1: net_sales + discounts ≈ G1 gross (per store-day)
SELECT d.order_date, d.restaurant_id,
       d.net_sales_after_discounts + d.discount_amount_abs    AS expected_gross,
       g.gross_sales_total                                    AS g1_gross,
       ROUND( (d.net_sales_after_discounts + d.discount_amount_abs) - g.gross_sales_total, 2) AS diff
FROM biz_insights.gold_discounts_by_day d
JOIN biz_insights.gold_daily_sales_by_store g
  ON d.restaurant_id = g.restaurant_id AND d.order_date = g.order_date
ORDER BY d.order_date DESC
LIMIT 20;

