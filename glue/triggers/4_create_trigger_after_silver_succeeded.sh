#!/bin/bash

# Recreate to be safe
for J in gold_g1_daily_sales_by_store gold_g2_top_items_30d gold_g3_customer_snapshot gold_g4_discount_effects; do
  NAME="after_silver_${J}"
  aws glue delete-trigger --name "$NAME" >/dev/null 2>&1 || true

  aws glue create-trigger \
    --name "$NAME" \
    --type CONDITIONAL \
    --workflow-name biz-insights-daily \
    --predicate '{"Conditions":[{"JobName":"biz_insights_silver_build","State":"SUCCEEDED","LogicalOperator":"EQUALS"}]}' \
    --actions "[{\"JobName\":\"$J\"}]"
done

