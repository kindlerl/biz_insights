#!/bin/bash

aws glue create-trigger \
  --name start_now \
  --type ON_DEMAND \
  --workflow-name biz-insights-daily \
  --actions '[{"JobName":"biz_insights_silver_build"}]'

