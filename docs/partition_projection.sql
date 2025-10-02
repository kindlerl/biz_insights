-- Oldest date in this project is 04/20/2020, so set
-- oldest date to 2020-01-01
-- G1: gold_daily_sales_by_store
ALTER TABLE biz_insights.gold_daily_sales_by_store
SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_date.type'='date',
  'projection.order_date.format'='yyyy-MM-dd',
  'projection.order_date.range'='2020-01-01,NOW',   -- set start to (on/before) your earliest order_date
  'projection.order_date.interval'='1',
  'projection.order_date.unit'='DAYS',
  'storage.location.template'='s3://bus-insights-dev-us-east-1/gold/daily_sales_by_store/order_date=${order_date}/'
);

-- G2: gold_item_sales_by_day
ALTER TABLE biz_insights.gold_item_sales_by_day
SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_date.type'='date',
  'projection.order_date.format'='yyyy-MM-dd',
  'projection.order_date.range'='2020-01-01,NOW',
  'projection.order_date.interval'='1',
  'projection.order_date.unit'='DAYS',
  'storage.location.template'='s3://bus-insights-dev-us-east-1/gold/item_sales_by_day/order_date=${order_date}/'
);

-- G4 (discounts by day) - gold_discounts_by_day
ALTER TABLE biz_insights.gold_discounts_by_day
SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_date.type'='date',
  'projection.order_date.format'='yyyy-MM-dd',
  'projection.order_date.range'='2020-01-01,NOW',
  'projection.order_date.interval'='1',
  'projection.order_date.unit'='DAYS',
  'storage.location.template'='s3://bus-insights-dev-us-east-1/gold/discounts_by_day/order_date=${order_date}/'
);

-- G3 (customer facts) - gold_customer_facts
ALTER TABLE biz_insights.gold_customer_facts
SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.snapshot_date.type'='date',
  'projection.snapshot_date.format'='yyyy-MM-dd',
  'projection.snapshot_date.range'='2020-01-01,NOW',
  'projection.snapshot_date.interval'='1',
  'projection.snapshot_date.unit'='DAYS',
  'storage.location.template'='s3://bus-insights-dev-us-east-1/gold/customer_facts/snapshot_date=${snapshot_date}/'
);

