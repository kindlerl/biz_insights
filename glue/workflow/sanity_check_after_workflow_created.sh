#!/bin/bash

# See the workflow DAG (jobs/crawler appear in the graph)
aws glue get-workflow --name biz-insights-daily --include-graph --query 'Workflow.Graph'

# List triggers attached to the workflow
aws glue list-triggers --workflow-name biz-insights-daily --query 'TriggerNames'

# Inspect each trigger (type, actions, predicate, enabled state)
aws glue get-trigger --name start_daily_0100utc          --query 'Trigger'
aws glue get-trigger --name after_silver_succeeded       --query 'Trigger'
aws glue get-trigger --name after_all_gold_crawler       --query 'Trigger'

