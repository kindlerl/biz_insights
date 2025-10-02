-- 1) Daily sales trends (from G1)
--    • Per restaurant, per day grain
--    • Select orders between the most recent order date and 90 days prior to that
--    • Include loyalty sales, percent loyalty sales
CREATE OR REPLACE VIEW biz_insights.v_sales_trends_daily AS
SELECT
  restaurant_id,
  order_date,
  gross_sales_total,
  orders_count,
  items_count,
  CASE 
    WHEN orders_count > 0 
        THEN gross_sales_total / orders_count 
        ELSE NULL 
  END AS avg_order_value,
  loyalty_sales,
  non_loyalty_sales,
  CASE 
    WHEN gross_sales_total > 0 
        THEN loyalty_sales / gross_sales_total 
        ELSE 0 
  END AS loyalty_sales_pct
FROM 
  biz_insights.gold_daily_sales_by_store
-- date predicate so projection prunes
WHERE 
  order_date BETWEEN date_add('day', -90, (SELECT max(order_date) FROM biz_insights.gold_daily_sales_by_store))
             AND (SELECT max(order_date) FROM biz_insights.gold_daily_sales_by_store);

-- 2) Top items (rolling last 30 days anchored to data max; from G2)
--    • Find the top N items (by sales), per store, over the last 30 days
--    • Include the total number of items and the total sales
--    • Include a ranking to ease a query of finding "the top N stores"
CREATE OR REPLACE VIEW biz_insights.v_top_items_30d AS
WITH max_order_date AS (SELECT max(order_date) AS most_recent_order_date FROM biz_insights.gold_item_sales_by_day)
SELECT
  g.restaurant_id,
  g.item_category,
  g.item_name,
  SUM(g.units_sold) AS units_30d,
  CAST(SUM(g.sales_total) AS DECIMAL(14,2)) AS sales_30d,
  ROW_NUMBER() OVER (PARTITION BY g.restaurant_id ORDER BY SUM(g.sales_total) DESC) AS rn_store
FROM biz_insights.gold_item_sales_by_day g
WHERE g.order_date BETWEEN date_add('day', -29, (SELECT most_recent_order_date FROM max_order_date)) AND (SELECT most_recent_order_date FROM max_order_date)
GROUP BY g.restaurant_id, g.item_category, g.item_name;

-- 3) Loyalty mix by day (from G1)
--    • Get daily loyalty vs non-loyalty stats
--    • Include perentage loyalty orders as well
CREATE OR REPLACE VIEW biz_insights.v_loyalty_mix_daily AS
SELECT
  restaurant_id,
  order_date,
  loyalty_sales,
  non_loyalty_sales,
  loyalty_orders,
  non_loyalty_orders,
  CASE WHEN (loyalty_orders + non_loyalty_orders) > 0
       THEN CAST(loyalty_orders AS DOUBLE) / (loyalty_orders + non_loyalty_orders)
       ELSE 0 END AS loyalty_orders_pct
FROM biz_insights.gold_daily_sales_by_store
WHERE order_date BETWEEN date_add('day', -90, (SELECT max(order_date) FROM biz_insights.gold_daily_sales_by_store))
                    AND (SELECT max(order_date) FROM biz_insights.gold_daily_sales_by_store);

-- 4) Discounts by day (from G4)
CREATE OR REPLACE VIEW biz_insights.v_discounts_by_day AS
SELECT
  restaurant_id,
  order_date,
  pre_discount_sales,
  discount_amount_abs,
  net_sales_after_discounts,
  pct_orders_discounted,
  pct_sales_discounted
FROM biz_insights.gold_discounts_by_day
WHERE order_date BETWEEN date_add('day', -90, (SELECT max(order_date) FROM biz_insights.gold_discounts_by_day))
                    AND (SELECT max(order_date) FROM biz_insights.gold_discounts_by_day);

-- 5) Latest customer snapshot (from G3)
CREATE OR REPLACE VIEW biz_insights.v_customer_facts_latest AS
SELECT g.*
FROM biz_insights.gold_customer_facts g
WHERE g.snapshot_date = (SELECT max(snapshot_date) FROM biz_insights.gold_customer_facts);

