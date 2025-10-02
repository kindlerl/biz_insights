#!/bin/bash

echo "Job: bus_insights_ingest_to_bronze"
aws glue get-job --job-name bus_insights_ingest_to_bronze \
  --query 'Job.DefaultArguments'

echo "Job: biz_insights_silver_build"
aws glue get-job --job-name biz_insights_silver_build \
  --query 'Job.DefaultArguments'

echo "Job: gold_g1_daily_sales_by_store"
aws glue get-job --job-name gold_g1_daily_sales_by_store \
  --query 'Job.DefaultArguments'

echo "Job: gold_g2_item_sales_by_day"
aws glue get-job --job-name gold_g2_item_sales_by_day \
  --query 'Job.DefaultArguments'

echo "Job: gold_g3_customer_facts"
aws glue get-job --job-name gold_g3_customer_facts \
  --query 'Job.DefaultArguments'

echo "Job: gold_g4_discounts_by_day"
aws glue get-job --job-name gold_g4_discounts_by_day \
  --query 'Job.DefaultArguments'

