CREATE OR REPLACE VIEW biz_insights.v_sales_monthly_store AS
SELECT
  restaurant_id,
  date_trunc('month', order_date) AS month,
  CAST(SUM(gross_sales_total) AS DECIMAL(14,2)) AS gross_month,
  SUM(orders_count) AS orders_month
FROM biz_insights.gold_daily_sales_by_store
GROUP BY restaurant_id, date_trunc('month', order_date);

