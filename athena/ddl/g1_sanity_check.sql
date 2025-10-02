-- If NOT using Partition Projection in Athena, then we need to
-- build the partition table in Glue Catalog by running the following:
MSCK REPAIR TABLE biz_insights.gold_daily_sales_by_store;
-- ========================================
SHOW PARTITIONS biz_insights.gold_daily_sales_by_store;

SELECT order_date, COUNT(*) AS rows
FROM biz_insights.gold_daily_sales_by_store
GROUP BY order_date
ORDER BY order_date DESC;

SELECT * FROM biz_insights.gold_daily_sales_by_store
ORDER BY order_date DESC
LIMIT 10;
