CREATE EXTERNAL TABLE IF NOT EXISTS biz_insights.gold_discounts_by_day (
  restaurant_id string,
  orders_count bigint,
  discounted_orders bigint,
  count_discounts bigint,
  discount_amount_abs decimal(14,2),
  pre_discount_sales decimal(14,2),
  net_sales_after_discounts decimal(14,2),
  pct_orders_discounted double,
  pct_sales_discounted double
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://bus-insights-dev-us-east-1/gold/discounts_by_day/';

