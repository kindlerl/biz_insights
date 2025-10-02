#!/bin/bash

aws glue get-workflow --name biz-insights-daily --include-graph --query 'Workflow.Graph'

