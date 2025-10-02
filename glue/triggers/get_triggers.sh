#!/bin/bash

aws glue get-triggers \
  --query 'Triggers[?WorkflowName==`biz-insights-daily`].{Name:Name,Type:Type,State:State,Predicate:Predicate,Actions:Actions}' \
  --region us-east-1

