CREATE EXTERNAL TABLE IF NOT EXISTS biz_insights.gold_daily_sales_by_store (
  restaurant_id string,
  item_sales decimal(14,2),
  option_sales decimal(14,2),
  gross_sales_total decimal(14,2),
  orders_count bigint,
  items_count bigint,
  loyalty_sales decimal(14,2),
  loyalty_orders bigint,
  non_loyalty_sales decimal(14,2),
  non_loyalty_orders bigint
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://bus-insights-dev-us-east-1/gold/daily_sales_by_store/';

MSCK REPAIR TABLE biz_insights.gold_daily_sales_by_store;
SHOW PARTITIONS biz_insights.gold_daily_sales_by_store;
SELECT * FROM biz_insights.gold_daily_sales_by_store
ORDER BY order_date DESC
LIMIT 10;

