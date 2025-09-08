import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

args   = getResolvedOptions(sys.argv, ['BUCKET'])
BUCKET = args['BUCKET'].strip().strip('"').strip("'")
spark  = SparkSession.builder.appName("silver-build").getOrCreate()

read = lambda name: spark.read.parquet(f"s3://{BUCKET}/bronze/{name}/")

# order_items → add order_total, order_date; cast types
items_sv = (read("order_items")
  .withColumn("item_price", F.col("item_price").cast("decimal(10,2)"))
  .withColumn("item_quantity", F.col("item_quantity").cast("int"))
  .withColumn("is_loyalty", F.col("is_loyalty").cast("boolean"))
  .withColumn("creation_time_utc", F.col("creation_time_utc").cast("timestamp"))
  .withColumn("order_date", F.to_date("creation_time_utc"))
  .withColumn("order_total", (F.col("item_price")*F.col("item_quantity")).cast("decimal(12,2)"))
  .withColumn("ingestion_date", F.col("ingestion_date").cast("date"))
)
(items_sv.write.mode("overwrite").option("compression","snappy")
  .parquet(f"s3://{BUCKET}/silver/order_items/"))

# order_items_options
opts_sv = (read("order_items_options")
  .withColumn("option_price", F.col("option_price").cast("decimal(10,2)"))
  .withColumn("option_quantity", F.col("option_quantity").cast("int"))
  .withColumn("ingestion_date", F.col("ingestion_date").cast("date"))
)
(opts_sv.write.mode("overwrite").option("compression","snappy")
  .parquet(f"s3://{BUCKET}/silver/order_items_options/"))

# date_dim
date_sv = (read("date_dim")
  .withColumn("year", F.col("year").cast("int"))
  .withColumn("week", F.col("week").cast("int"))
  .withColumn("is_weekend", F.col("is_weekend").cast("boolean"))
  .withColumn("is_holiday", F.col("is_holiday").cast("boolean"))
  .withColumn("ingestion_date", F.col("ingestion_date").cast("date"))
)
(date_sv.write.mode("overwrite").option("compression","snappy")
  .parquet(f"s3://{BUCKET}/silver/date_dim/"))

print("Silver build complete.")

