-- 1) silver_order_items
CREATE EXTERNAL TABLE IF NOT EXISTS biz_insights.silver_order_items (
  app_name            string,
  restaurant_id       string,
  creation_time_utc   timestamp,
  order_id            string,
  user_id             string,
  printed_card_number string,
  is_loyalty          boolean,
  currency            string,
  lineitem_id         string,
  item_category       string,
  item_name           string,
  item_price          decimal(10,2),
  item_quantity       int,
  order_total         decimal(12,2),
  order_date          date,
  ingestion_date      date
)
STORED AS PARQUET
LOCATION 's3://bus-insights-dev-us-east-1/silver/order_items/';

-- 2) silver_order_items_options
CREATE EXTERNAL TABLE IF NOT EXISTS biz_insights.silver_order_items_options (
  order_id        string,
  lineitem_id     string,
  option_name     string,
  option_price    decimal(10,2),
  option_quantity int,
  ingestion_date  date
)
STORED AS PARQUET
LOCATION 's3://bus-insights-dev-us-east-1/silver/order_items_options/';

-- 3) silver_date_dim
CREATE EXTERNAL TABLE IF NOT EXISTS biz_insights.silver_date_dim (
  date_key      date,
  year          int,
  month         int,
  day           int,
  week          int,
  day_of_week   string,
  is_weekend    boolean,
  is_holiday    boolean,
  holiday_name  string,
  ingestion_date date
)
STORED AS PARQUET
LOCATION 's3://bus-insights-dev-us-east-1/silver/date_dim/';

