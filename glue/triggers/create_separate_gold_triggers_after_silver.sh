#!/bin/bash

aws glue create-trigger --name after_silver_g1 --type CONDITIONAL --workflow-name biz-insights-daily \
  --predicate '{"Conditions":[{"LogicalOperator":"EQUALS","JobName":"biz_insights_silver_build","State":"SUCCEEDED"}]}' \
  --actions '[{"JobName":"gold_g1_daily_sales_by_store"}]' \
  --start-on-creation --region us-east-1

aws glue create-trigger --name after_silver_g2 --type CONDITIONAL --workflow-name biz-insights-daily \
  --predicate '{"Conditions":[{"LogicalOperator":"EQUALS","JobName":"biz_insights_silver_build","State":"SUCCEEDED"}]}' \
  --actions '[{"JobName":"gold_g2_item_sales_by_day"}]' \
  --start-on-creation --region us-east-1

aws glue create-trigger --name after_silver_g3 --type CONDITIONAL --workflow-name biz-insights-daily \
  --predicate '{"Conditions":[{"LogicalOperator":"EQUALS","JobName":"biz_insights_silver_build","State":"SUCCEEDED"}]}' \
  --actions '[{"JobName":"gold_g3_customer_facts"}]' \
  --start-on-creation --region us-east-1

aws glue create-trigger --name after_silver_g4 --type CONDITIONAL --workflow-name biz-insights-daily \
  --predicate '{"Conditions":[{"LogicalOperator":"EQUALS","JobName":"biz_insights_silver_build","State":"SUCCEEDED"}]}' \
  --actions '[{"JobName":"gold_g4_discounts_by_day"}]' \
  --start-on-creation --region us-east-1

