SELECT max(order_date) FROM biz_insights.gold_daily_sales_by_store;
SELECT * FROM biz_insights.gold_daily_sales_by_store ORDER BY order_date DESC LIMIT 5;

