#!/bin/bash

# Pick a TARGET_DATE that is max(order_date)+1 to create a new order date
aws glue start-job-run \
  --job-name bronze_synthetic_day \
  --arguments file://<(cat <<'JSON'
{
  "--BUCKET": "bus-insights-dev-us-east-1",
  "--SOURCE_DATE": "2024-02-13",
  "--TARGET_DATE": "2024-02-22",
  "--LIMIT": "2000"
}
JSON
)

