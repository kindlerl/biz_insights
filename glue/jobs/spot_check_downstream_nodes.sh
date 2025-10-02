#!/bin/bash

aws glue list-job-runs --job-name biz_insights_silver_build    --max-results 1 --query 'JobRuns[0].JobRunState'
aws glue list-job-runs --job-name gold_g1_daily_sales_by_store --max-results 1 --query 'JobRuns[0].JobRunState'
aws glue list-job-runs --job-name gold_g2_top_items_30d        --max-results 1 --query 'JobRuns[0].JobRunState'
aws glue list-job-runs --job-name gold_g3_customer_snapshot    --max-results 1 --query 'JobRuns[0].JobRunState'
aws glue list-job-runs --job-name gold_g4_discount_effects     --max-results 1 --query 'JobRuns[0].JobRunState'
aws glue get-crawler-metrics --crawler-name-list gold_partitions_crawler

