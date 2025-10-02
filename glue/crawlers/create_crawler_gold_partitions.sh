#!/bin/bash

aws glue create-crawler \
  --name gold_partitions_crawler \
  --role biz-insights-glue-athena-s3-role \
  --database-name biz_insights \
  --targets '{
    "CatalogTargets": [{
      "DatabaseName": "biz_insights",
      "Tables": [
        "gold_daily_sales_by_store",
        "gold_item_sales_by_day",
        "gold_customer_facts",
        "gold_discounts_by_day"
      ]
    }]
  }' \
  --schema-change-policy UpdateBehavior=UPDATE_IN_DATABASE,DeleteBehavior=LOG \
  --configuration "{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"}}}" \
  --region us-east-1

