-- (one-time) create table
CREATE EXTERNAL TABLE IF NOT EXISTS biz_insights.gold_customer_facts (
  user_id string,
  first_order_date date,
  last_order_date date,
  lifetime_orders bigint,
  lifetime_gross_sales decimal(14,2),
  avg_order_value decimal(14,2),
  loyalty_orders bigint,
  non_loyalty_orders bigint,
  last_90d_orders bigint,
  last_90d_sales decimal(14,2),
  prev_90d_sales decimal(14,2),
  delta_90d_pct double,
  days_since_last_order int,
  avg_interorder_gap_days double,
  recency_score int,
  frequency_score int,
  monetary_score int,
  clv_band string,
  segment_label string
)
PARTITIONED BY (snapshot_date date)
STORED AS PARQUET
LOCATION 's3://bus-insights-dev-us-east-1/gold/customer_facts/';

-- enable partition projection (hands-off forever)
ALTER TABLE biz_insights.gold_customer_facts
SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.snapshot_date.type'='date',
  'projection.snapshot_date.format'='yyyy-MM-dd',
  'projection.snapshot_date.range'='2020-01-01,2024-02-21',  -- covers your data span; adjust if needed
  'projection.snapshot_date.interval'='1',
  'projection.snapshot_date.unit'='DAYS',
  'storage.location.template'='s3://bus-insights-dev-us-east-1/gold/customer_facts/snapshot_date=${snapshot_date}/'
);

