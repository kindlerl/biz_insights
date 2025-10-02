#!/bin/bash

# Start (you already did, but keeping here)
RUN_ID=$(aws glue start-workflow-run --name biz-insights-daily --query RunId --output text --region us-east-1)

# Poll overall status
aws glue get-workflow-run --name biz-insights-daily --run-id "$RUN_ID" \
  --query 'Run.{Status:Status,StartedOn:StartedOn,CompletedOn:CompletedOn}' --region us-east-1

# See the most recent run per job (fresh timestamps should appear for all)
for J in bus_insights_ingest_to_bronze biz_insights_silver_build \
         gold_g1_daily_sales_by_store gold_g2_item_sales_by_day \
         gold_g3_customer_facts gold_g4_discounts_by_day; do
  aws glue get-job-runs --job-name "$J" --max-results 1 \
    --query 'JobRuns[0].{Job:JobName,State:JobRunState,Started:StartedOn,Duration:ExecutionTime}' \
    --region us-east-1 || echo "Missing: $J"
done

# Crawler confirmations
aws glue get-crawler --name biz-insights-glue-crawler    --query '{State:State,LastCrawl:LastCrawl}' --region us-east-1
aws glue get-crawler --name gold_partitions_crawler      --query '{State:State,LastCrawl:LastCrawl}' --region us-east-1

