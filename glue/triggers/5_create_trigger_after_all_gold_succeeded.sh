#!/bin/bash

# Make it idempotent
aws glue delete-trigger --name after_all_gold_crawler >/dev/null 2>&1 || true

# Crawler trigger: AFTER ALL Gold SUCCEEDED → start crawler
aws glue create-trigger \
  --name after_all_gold_crawler \
  --type CONDITIONAL \
  --workflow-name biz-insights-daily \
  --start-on-creation \
  --actions '[{"CrawlerName":"gold_partitions_crawler"}]' \
  --predicate '{
    "Logical":"AND",
    "Conditions":[
      {"LogicalOperator":"EQUALS","JobName":"gold_g1_daily_sales_by_store","State":"SUCCEEDED"},
      {"LogicalOperator":"EQUALS","JobName":"gold_g2_top_items_30d","State":"SUCCEEDED"},
      {"LogicalOperator":"EQUALS","JobName":"gold_g3_customer_snapshot","State":"SUCCEEDED"},
      {"LogicalOperator":"EQUALS","JobName":"gold_g4_discount_effects","State":"SUCCEEDED"}
    ]
  }'

