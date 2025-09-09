CREATE EXTERNAL TABLE IF NOT EXISTS biz_insights.gold_item_sales_by_day (
  restaurant_id string,
  item_category string,
  item_name string,
  units_sold bigint,
  item_sales_only decimal(14,2),
  attached_options_sales decimal(14,2),
  sales_total decimal(14,2)
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://bus-insights-dev-us-east-1/gold/item_sales_by_day/';

-- After creating the table...
MSCK REPAIR TABLE biz_insights.gold_item_sales_by_day;
-- (daily going forward, prefer a quick add)
-- ALTER TABLE biz_insights.gold_item_sales_by_day
-- ADD IF NOT EXISTS PARTITION (order_date='YYYY-MM-DD')
-- LOCATION 's3://bus-insights-dev-us-east-1/gold/item_sales_by_day/order_date=YYYY-MM-DD/';

