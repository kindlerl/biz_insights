CREATE OR REPLACE VIEW biz_insights.v_churn_risk_latest AS
WITH latest AS (
  SELECT MAX(CAST(snapshot_date AS DATE)) AS max_snap
  FROM biz_insights.gold_customer_facts
)
SELECT
  g.user_id,
  g.days_since_last_order,
  g.avg_interorder_gap_days,
  g.last_90d_orders_count AS last_90d_orders,  -- alias to expected name
  g.last_90d_sales,
  g.prev_90d_sales,
  g.delta_90d_pct,
  g.recency_score,
  g.frequency_score,
  g.monetary_score,
  CASE
    WHEN g.recency_score <= 2
      OR g.days_since_last_order > 45
      OR (g.last_90d_orders_count = 0 AND g.prev_90d_sales > 0)
      OR g.delta_90d_pct <= -0.5
    THEN 'High'
    WHEN g.days_since_last_order > 30
      OR g.delta_90d_pct <= -0.25
    THEN 'Medium'
    ELSE 'Low'
  END AS risk_level
FROM biz_insights.gold_customer_facts g
CROSS JOIN latest
WHERE CAST(g.snapshot_date AS DATE) = latest.max_snap;
