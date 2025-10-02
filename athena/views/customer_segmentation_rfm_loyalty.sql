CREATE OR REPLACE VIEW biz_insights.v_customer_segments_latest AS
WITH latest AS (
  SELECT *
  FROM biz_insights.gold_customer_facts
  WHERE snapshot_date = (SELECT max(snapshot_date) FROM biz_insights.gold_customer_facts)
)
SELECT
  user_id,
  (loyalty_orders > 0) AS is_loyalty_member,
  recency_score, frequency_score, monetary_score, segment_label,
  lifetime_gross_sales, avg_order_value,
  last_90d_orders, last_90d_sales, days_since_last_order
FROM latest;

