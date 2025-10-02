#!/bin/bash

aws glue create-trigger \
  --name t_start_bronze \
  --type ON_DEMAND \
  --workflow-name biz-insights-daily \
  --actions '[{"JobName":"bus_insights_ingest_to_bronze"}]'
