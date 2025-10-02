CREATE OR REPLACE VIEW biz_insights.v_loyalty_vs_nonloyalty AS
WITH c AS (
  SELECT
    (loyalty_orders > 0) AS is_loyalty_member,
    lifetime_gross_sales,
    avg_order_value,
    lifetime_orders,
    last_90d_orders, last_90d_sales
  FROM biz_insights.gold_customer_facts
  WHERE snapshot_date = (SELECT max(snapshot_date) FROM biz_insights.gold_customer_facts)
)
SELECT
  is_loyalty_member,
  CAST(AVG(lifetime_gross_sales) AS DECIMAL(14,2)) AS avg_clv,
  CAST(AVG(avg_order_value)      AS DECIMAL(14,2)) AS avg_aov,
  CAST(AVG(CASE WHEN lifetime_orders >= 2 THEN 1 ELSE 0 END) AS DOUBLE) AS repeat_rate,
  CAST(AVG(last_90d_orders) AS DOUBLE) AS avg_orders_90d,
  CAST(AVG(last_90d_sales)  AS DECIMAL(14,2)) AS avg_sales_90d
FROM c
GROUP BY is_loyalty_member;

