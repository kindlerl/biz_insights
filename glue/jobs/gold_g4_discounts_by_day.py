# glue/jobs/gold_g4_discounts_by_day.py

import sys, datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

# -------- args --------
required = ['BUCKET']
optional = ['PROCESS_DATE','FULL_REFRESH']
present = required + [k for k in optional if f'--{k}' in sys.argv]
args = getResolvedOptions(sys.argv, present)

BUCKET       = args['BUCKET'].strip().strip('"').strip("'")
FULL_REFRESH = args.get('FULL_REFRESH','false').lower() == 'true'
PROCESS_DATE = args.get('PROCESS_DATE') or (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

# -------- spark --------
spark = (SparkSession.builder.appName("gold-discounts-by-day").getOrCreate())
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

read = lambda p: spark.read.parquet(f"s3://{BUCKET}/{p}")

# -------- load silver --------
order_items = read("silver/order_items").select(
    "restaurant_id","order_date","order_id","lineitem_id",
    "item_quantity","order_total"  # order_total = item_price * item_quantity, calculated in Silver build
)
order_options  = read("silver/order_items_options").select(
    "order_id","lineitem_id","option_price","option_quantity"
)

# Filter items to PROCESS_DATE in daily mode
if not FULL_REFRESH:
    order_items = order_items.filter(F.col("order_date") == F.lit(PROCESS_DATE))

# -------- options → split positive vs discounts (negative) per line --------
options_with_extended = (order_options
    .withColumn("extended", F.col("option_price") * F.col("option_quantity"))
    .withColumn("is_discount_line", F.col("option_price") < 0)
)

options_line = (options_with_extended
    .groupBy("order_id","lineitem_id")
    .agg(
        F.sum(F.when(F.col("extended") >= 0, F.col("extended")).otherwise(F.lit(0.0))).alias("options_regular_total"), # regular price (maybe no charge (zero))
        F.sum(F.when(F.col("extended") < 0,  F.col("extended")).otherwise(F.lit(0.0))).alias("options_discounts_total"),  # negative (discount) value
        F.sum(F.when(F.col("is_discount_line"), F.lit(1)).otherwise(F.lit(0))).alias("discount_count_line")
    )
)

# -------- enrich items with options split --------
orders_with_options_totals = (order_items
    .join(options_line, ["order_id","lineitem_id"], "left")
    .withColumn("options_regular_total", F.coalesce(F.col("options_regular_total"), F.lit(0.0)))
    .withColumn("options_discounts_total",  F.coalesce(F.col("options_discounts_total"),  F.lit(0.0)))  # negative
    .withColumn("discount_count_line", F.coalesce(F.col("discount_count_line"), F.lit(0)))
    # pre-discount = items + normal options
    .withColumn("line_pre_total", (F.col("order_total") + F.col("options_regular_total")).cast("decimal(18,4)"))
    # net after discounts = pre + discounts (negative value)
    .withColumn("line_net_total", (F.col("order_total") + F.col("options_regular_total") + F.col("options_discounts_total")).cast("decimal(18,4)"))
    # flag if this line had any discount
    .withColumn("has_discount_line", F.col("discount_count_line") > 0)
)

# -------- collapse to ORDER grain by date --------
orders = (orders_with_options_totals
    .groupBy("restaurant_id","order_date","order_id")
    .agg(
        F.sum("line_pre_total").cast("decimal(18,4)").alias("order_pre_total"),
        F.sum("line_net_total").cast("decimal(18,4)").alias("order_net_total"),
        # discounts_total is negative; flip sign to get positive "amount discounted"
        F.sum(-F.col("options_discounts_total")).cast("decimal(18,4)").alias("order_discount_abs"),
        F.sum("discount_count_line").alias("order_discount_count"),
        F.max(F.when(F.col("has_discount_line"), F.lit(1)).otherwise(F.lit(0))).alias("has_discount_order_int")
    )
    .withColumn("has_discount_order", F.col("has_discount_order_int") == 1)
    .drop("has_discount_order_int")
)

# -------- collapse to STORE-DAY grain --------
daily = (orders
    .groupBy("restaurant_id","order_date")
    .agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.sum(F.when(F.col("has_discount_order"), 1).otherwise(0)).alias("discounted_orders"),
        F.sum("order_discount_count").alias("count_discounts"),
        F.sum("order_discount_abs").cast("decimal(18,4)").alias("discount_amount_abs"),
        F.sum("order_pre_total").cast("decimal(18,4)").alias("pre_discount_sales"),
        F.sum("order_net_total").cast("decimal(18,4)").alias("net_sales_after_discounts")
    )
    .withColumn(
        "pct_orders_discounted",
        F.when(F.col("orders_count") > 0,
               (F.col("discounted_orders") / F.col("orders_count")).cast("double"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_sales_discounted",
        F.when(F.col("pre_discount_sales") > 0,
               (F.col("discount_amount_abs") / F.col("pre_discount_sales")).cast("double"))
         .otherwise(F.lit(0.0))
    )
    # final money precision to (14,2)
    .select(
        "restaurant_id","order_date","orders_count","discounted_orders","count_discounts",
        F.col("discount_amount_abs").cast("decimal(14,2)").alias("discount_amount_abs"),
        F.col("pre_discount_sales").cast("decimal(14,2)").alias("pre_discount_sales"),
        F.col("net_sales_after_discounts").cast("decimal(14,2)").alias("net_sales_after_discounts"),
        "pct_orders_discounted","pct_sales_discounted"
    )
)

# -------- write partitioned --------
(daily
  .write.mode("overwrite")
  .option("compression","snappy")
  .partitionBy("order_date")
  .parquet(f"s3://{BUCKET}/gold/discounts_by_day/"))

print("G4 complete.", "Mode=FULL" if FULL_REFRESH else f"PROCESS_DATE={PROCESS_DATE}")

