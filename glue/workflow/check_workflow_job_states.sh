#!/bin/bash

for J in bus_insights_ingest_to_bronze biz_insights_silver_build \
         gold_g1_daily_sales_by_store gold_g2_item_sales_by_day \
         gold_g3_customer_facts gold_g4_discounts_by_day; do
  aws glue get-job-runs --job-name "$J" --max-results 1 \
    --query 'JobRuns[0].{Job:JobName,State:JobRunState,Started:StartedOn,Duration:ExecutionTime}' || echo "Missing: $J"
done

