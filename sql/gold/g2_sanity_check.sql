-- partitions present?
SHOW PARTITIONS biz_insights.gold_item_sales_by_day;

-- top items by sales for the most recent date in G2
WITH maxd AS (SELECT max(order_date) AS d FROM biz_insights.gold_item_sales_by_day)
SELECT restaurant_id, item_category, item_name, units_sold, sales_total
FROM biz_insights.gold_item_sales_by_day g
JOIN maxd ON g.order_date = maxd.d
ORDER BY sales_total DESC
LIMIT 20;

-- reconcile: item + options totals should equal sales_total (tolerance for decimals = 0)
SELECT max(abs((item_sales_only + attached_options_sales) - sales_total)) AS max_diff
FROM biz_insights.gold_item_sales_by_day;

