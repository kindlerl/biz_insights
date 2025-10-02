CREATE OR REPLACE VIEW biz_insights.v_sales_monthly_category AS
SELECT
  restaurant_id,
  item_category,
  date_trunc('month', order_date) AS month,
  CAST(SUM(sales_total) AS DECIMAL(14,2)) AS sales_month,
  SUM(units_sold) AS units_month
FROM biz_insights.gold_item_sales_by_day
GROUP BY restaurant_id, item_category, date_trunc('month', order_date);

