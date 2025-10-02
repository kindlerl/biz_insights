#!/bin/bash

aws glue create-trigger \
  --name start_daily_0100utc \
  --type SCHEDULED \
  --workflow-name biz-insights-daily \
  --schedule "cron(0 1 * * ? *)" \
  --actions '[{"JobName":"t_start_bronze"}]'

