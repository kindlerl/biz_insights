#!/bin/bash

aws glue create-trigger \
  --name after_bronze_run_crawler \
  --type CONDITIONAL \
  --workflow-name biz-insights-daily \
  --predicate '{"Conditions":[{"JobName":"bus_insights_ingest_to_bronze","State":"SUCCEEDED","LogicalOperator":"EQUALS"}]}' \
  --actions '[{"CrawlerName":"biz-insights-glue-crawler"}]'

