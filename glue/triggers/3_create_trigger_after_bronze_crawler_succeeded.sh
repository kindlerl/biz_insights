#!/bin/bash

# Recreate to be safe/idempotent
aws glue delete-trigger --name after_bronze_succeeded >/dev/null 2>&1 || true

# Kick off the silver build after the bronze crawler completes
aws glue create-trigger \
  --name after_bronze_succeeded \
  --type CONDITIONAL \
  --workflow-name biz-insights-daily \
  --predicate '{"Conditions":[{"CrawlerName":"biz-insights-glue-crawler","CrawlState":"SUCCEEDED","LogicalOperator":"EQUALS"}]}' \
  --actions '[{"JobName":"biz_insights_silver_build"}]'

