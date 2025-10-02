CREATE OR REPLACE VIEW biz_insights.v_churn_risk_latest AS
WITH g AS (
  SELECT *
  FROM biz_insights.gold_customer_facts
  WHERE snapshot_date = (SELECT max(snapshot_date) FROM biz_insights.gold_customer_facts)
)
SELECT
  user_id,
  days_since_last_order,
  avg_interorder_gap_days,
  last_90d_orders, last_90d_sales, prev_90d_sales, delta_90d_pct,
  recency_score, frequency_score, monetary_score,
  CASE
    WHEN recency_score <= 2
       OR days_since_last_order > 45
       OR (last_90d_orders = 0 AND prev_90d_sales > 0)
       OR delta_90d_pct <= -0.5
      THEN 'High'
    WHEN days_since_last_order > 30
       OR delta_90d_pct <= -0.25
      THEN 'Medium'
    ELSE 'Low'
  END AS risk_level
FROM g;

