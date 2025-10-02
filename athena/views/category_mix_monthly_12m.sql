CREATE OR REPLACE VIEW biz_insights.v_category_mix_monthly_12m AS
WITH maxd AS (
  SELECT CAST(MAX(CAST(order_date AS DATE)) AS DATE) AS d
  FROM biz_insights.gold_item_sales_by_day
),
mc AS (
  SELECT
    restaurant_id,
    item_category,
    date_trunc('month', CAST(order_date AS DATE)) AS month,
    CAST(SUM(sales_total) AS DECIMAL(14,2)) AS sales_month
  FROM biz_insights.gold_item_sales_by_day t
  CROSS JOIN maxd
  WHERE CAST(t.order_date AS DATE)
        BETWEEN date_add('month', CAST(-11 AS BIGINT), maxd.d) AND maxd.d
  GROUP BY
    restaurant_id,
    item_category,
    date_trunc('month', CAST(order_date AS DATE))
),
tot AS (
  SELECT restaurant_id, month, SUM(sales_month) AS total_month
  FROM mc
  GROUP BY restaurant_id, month
)
SELECT
  mc.restaurant_id,
  mc.month,
  mc.item_category,
  mc.sales_month,
  CAST(mc.sales_month / NULLIF(tot.total_month, 0) AS DOUBLE) AS sales_share
FROM mc
JOIN tot
  ON mc.restaurant_id = tot.restaurant_id AND mc.month = tot.month;
