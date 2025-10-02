#!/bin/bash

RUN_ID=$(aws glue start-workflow-run --name biz-insights-daily --query 'RunId' --output text)

aws glue get-workflow-run --name biz-insights-daily --run-id "$RUN_ID" --query 'Run.Status'

