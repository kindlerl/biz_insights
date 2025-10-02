#!/bin/bash

aws glue start-job-run \
  --job-name bronze_synth_day \
  --arguments file://<(cat <<'JSON'
{
  "--BUCKET": "bus-insights-dev-us-east-1",
  "--SOURCE_DATE": "2024-02-21",
  "--TARGET_DATE": "2024-02-22",
  "--LIMIT": "2000"
}
JSON
)

