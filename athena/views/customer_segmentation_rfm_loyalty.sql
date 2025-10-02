CREATE OR REPLACE VIEW biz_insights.v_customer_segments_latest AS
WITH latest AS (
  SELECT MAX(CAST(snapshot_date AS DATE)) AS max_snap
  FROM biz_insights.gold_customer_facts
)
SELECT
  user_id,
  (loyalty_orders > 0) AS is_loyalty_member,
  recency_score,
  frequency_score,
  monetary_score,
  segment_label,
  lifetime_gross_sales,
  avg_order_value,
  last_90d_orders_count AS last_90d_orders,  -- alias to expected name
  last_90d_sales,
  days_since_last_order
FROM biz_insights.gold_customer_facts g
CROSS JOIN latest
WHERE CAST(g.snapshot_date AS DATE) = latest.max_snap;
