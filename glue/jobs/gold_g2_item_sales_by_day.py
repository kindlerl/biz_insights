# glue/jobs/gold_g2_item_sales_by_day.py

import sys, datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

# ---------- args ----------
required = ['BUCKET']
optional = ['PROCESS_DATE','FULL_REFRESH']
present = required + [k for k in optional if f'--{k}' in sys.argv]
args = getResolvedOptions(sys.argv, present)

BUCKET        = args['BUCKET'].strip().strip('"').strip("'")
FULL_REFRESH  = args.get('FULL_REFRESH', 'false').lower() == 'true'
PROCESS_DATE  = args.get('PROCESS_DATE') or (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

# ---------- spark ----------
spark = (SparkSession.builder.appName("gold-item-sales-by-day").getOrCreate())
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

read = lambda p: spark.read.parquet(f"s3://{BUCKET}/{p}")

# ---------- load silver ----------
order_items = read("silver/order_items").select(
    "restaurant_id","order_date","order_id","lineitem_id",
    "item_category","item_name","item_quantity","order_total"
)
order_opts  = read("silver/order_items_options").select(
    "order_id","lineitem_id","option_price","option_quantity"
)

# Filter items to PROCESS_DATE for daily mode
if not FULL_REFRESH:
    order_items = order_items.filter(F.col("order_date") == F.lit(PROCESS_DATE))

# options → per-line options_total (can be negative for redemptions/discounts)
opts_line = (order_opts
    .groupBy("order_id","lineitem_id")
    .agg(F.sum(F.col("option_price") * F.col("option_quantity")).alias("options_total"))
)

# enrich line order_items with options
orders_with_options_total = (order_items
    .join(opts_line, ["order_id","lineitem_id"], "left")
    .withColumn("options_total", F.coalesce(F.col("options_total"), F.lit(0.0)))
)

# aggregate to restaurant × day × item
daily_item = (orders_with_options_total
    .groupBy("restaurant_id","order_date","item_category","item_name")
    .agg(
        F.sum("item_quantity").alias("units_sold"),
        F.sum("order_total").cast("decimal(14,2)").alias("item_sales_only"),
        F.sum("options_total").cast("decimal(14,2)").alias("attached_options_sales"),
        F.sum(F.col("order_total") + F.col("options_total")).cast("decimal(14,2)").alias("sales_total")
    )
)

# write (partitioned by order_date)
(daily_item
    .write.mode("overwrite")
    .option("compression","snappy")
    .partitionBy("order_date")
    .parquet(f"s3://{BUCKET}/gold/item_sales_by_day/"))

print("G2 complete.", "Mode=FULL" if FULL_REFRESH else f"PROCESS_DATE={PROCESS_DATE}")

