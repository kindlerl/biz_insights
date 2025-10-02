#!/bin/bash

aws glue start-job-run \
  --job-name bus_insights_ingest_to_bronze \
  --arguments '{
    "--BUCKET":"bus-insights-dev-us-east-1",
    "--PROCESS_DATE":"2024-02-22"
  }'

RUN_ID=$(aws glue start-workflow-run --name biz-insights-daily --query 'RunId' --output text)
aws glue get-workflow-run --name biz-insights-daily --run-id "$RUN_ID" --query 'Run.Status'

