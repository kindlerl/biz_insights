import sys, datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

args = getResolvedOptions(sys.argv, ['BUCKET','SOURCE_DATE','TARGET_DATE','LIMIT'])
BUCKET = args['BUCKET']
SRC = args['SOURCE_DATE']      # e.g. 2024-02-21
TGT = args['TARGET_DATE']      # e.g. 2024-02-22
LIM = int(args['LIMIT'])       # e.g. 2000

delta = (datetime.date.fromisoformat(TGT) - datetime.date.fromisoformat(SRC)).days

spark = SparkSession.builder.appName("bronze-synth-day").getOrCreate()
read = lambda name: spark.read.parquet(f"s3://{BUCKET}/bronze/{name}/")


# # 1) sample lines from the source day
# items_src = (read("order_items")
#   .filter(F.to_date("creation_time_utc")==F.lit(SRC))
#   .limit(LIM)
#   .cache())

# order_ids = [r.order_id for r in items_src.select("order_id").distinct().collect()]

# # # 2) shift timestamp by +delta days; refresh ingestion_date
# # items_new = (items_src
# #   .withColumn("creation_time_utc", F.expr(f"CAST({delta} AS INT) * INTERVAL 1 DAY + creation_time_utc"))
# #   .withColumn("ingestion_date", F.current_date()))

# # # 3) append to Bronze
# # (items_new.write.mode("append")
# #   .parquet(f"s3://{BUCKET}/bronze/order_items/"))

# # 2) shift to TARGET_DATE by adding 'delta' days (keeps date; time-of-day is fine for our use)
# items_new = (items_src
#   .withColumn("creation_time_utc", F.date_add(F.col("creation_time_utc"), delta))
#   .withColumn("ingestion_date", F.current_date()))

# # 3) append to Bronze **partitioned by ingestion_date**
# (items_new.write
#   .mode("append")
#   .option("compression","snappy")
#   .partitionBy("ingestion_date")
#   .parquet(f"s3://{BUCKET}/bronze/order_items/"))

# # 4) bring matching options; just refresh ingestion_date
# opts_src = read("order_items_options").filter(F.col("order_id").isin(order_ids))
# opts_new = opts_src.withColumn("ingestion_date", F.current_date())
# (opts_new.write.mode("append")
#   .parquet(f"s3://{BUCKET}/bronze/order_items_options/"))

# print(f"Synthesized {items_src.count()} order_items rows from {SRC} as {TGT}.")

# safe read paths
items_path = f"s3://{BUCKET}/bronze/order_items/ingestion_date=*/"  # only partitioned folders
opts_path  = f"s3://{BUCKET}/bronze/order_items_options/"

# 1) sample lines from the source day
items_src = (spark.read
  .option("mergeSchema","true")   # tolerant to minor evolution
  .parquet(items_path)
  .filter(F.to_date("creation_time_utc")==F.lit(SRC))
  .limit(LIM)
  .cache())

order_ids = [r.order_id for r in items_src.select("order_id").distinct().collect()]

# 2) shift timestamp by +delta days; refresh ingestion_date
items_new = (items_src
  .withColumn("creation_time_utc", F.date_add(F.col("creation_time_utc"), delta))
  .withColumn("ingestion_date", F.current_date()))

# 3) append to Bronze **partitioned by ingestion_date**
# write partitioned by ingestion_date so Athena sees it
(items_new.write
  .mode("append")
  .option("compression","snappy")
  .partitionBy("ingestion_date")
  .parquet(f"s3://{BUCKET}/bronze/order_items/"))

# options (usually not partitioned)
# opts_src = spark.read.parquet(opts_path).filter(F.col("order_id").isin(order_ids))
# opts_new = opts_src.withColumn("ingestion_date", F.current_date())
# (opts_new.write.mode("append").parquet(f"s3://{BUCKET}/bronze/order_items_options/"))

# OPTIONS: read only partitioned paths; then write partitioned as well
opts_src = (spark.read
    .option("mergeSchema", "true")
    .parquet(f"s3://{BUCKET}/bronze/order_items_options/ingestion_date=*/")
    .filter(F.col("order_id").isin(order_ids)))

opts_new = opts_src.withColumn("ingestion_date", F.current_date())

(opts_new.write
   .mode("append")
   .option("compression","snappy")
   .partitionBy("ingestion_date")
   .parquet(f"s3://{BUCKET}/bronze/order_items_options/"))
   
# avoid an action that forces full-file schema merge just for logging
print(f"Synthesized rows for SOURCE_DATE={SRC} -> TARGET_DATE={TGT}.")

