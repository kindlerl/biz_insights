CREATE OR REPLACE VIEW biz_insights.v_sales_with_holidays_daily AS
SELECT
  g.restaurant_id, g.order_date,
  g.gross_sales_total, g.orders_count,
  d.is_holiday, d.holiday_name, d.is_weekend
FROM biz_insights.gold_daily_sales_by_store g
JOIN biz_insights.silver_date_dim d
  ON d.date_key = g.order_date;

