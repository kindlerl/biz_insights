import sys, datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

# args = getResolvedOptions(sys.argv, ['BUCKET'])
# BUCKET = args['BUCKET'].strip().strip('"').strip("'")
# PROCESS_DATE = None
# if 'PROCESS_DATE' in args:
#     PROCESS_DATE = args['PROCESS_DATE']
# else:
#     PROCESS_DATE = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
# --- parse args (support optional flags safely) ---
required = ['BUCKET']
optional = ['PROCESS_DATE','FULL_REFRESH']
provided = required + [k for k in optional if f'--{k}' in sys.argv]
args = getResolvedOptions(sys.argv, provided)

BUCKET = args['BUCKET'].strip().strip('"').strip("'")
full_refresh = args.get('FULL_REFRESH', 'false').lower() == 'true'
process_date = args.get('PROCESS_DATE') or (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

spark = (SparkSession.builder
         .appName("gold-daily-sales-by-store")
         .getOrCreate())

# overwrite only the partition we write
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ---------- helpers ------------
def assert_true(cond, msg: str):
    if not cond:
        raise RuntimeError(f"ASSERT FAILED: {msg}")

def assert_has_columns(df, cols):
    missing = [c for c in cols if c not in df.columns]
    assert_true(len(missing) == 0, f"Missing columns: {missing}")

def assert_nonempty(df, msg="DataFrame is empty"):
    # Spark 3-safe emptiness check
    cnt = df.limit(1).count()
    assert_true(cnt > 0, msg)

def assert_partition_present(df, part_col: str, part_value_str: str):
    # Ensure the partition you intend to process actually exists
    has = (df.select(F.col(part_col).cast("string").alias("p"))
             .where(F.col("p") == part_value_str)
             .limit(1).count() > 0)
    assert_true(has, f"Partition {part_col}={part_value_str} not found in source")

def assert_no_nulls(df, cols):
    for c in cols:
        n = df.where(F.col(c).isNull()).limit(1).count()
        assert_true(n == 0, f"Column {c} contains NULLs unexpectedly")

read = lambda path: spark.read.parquet(f"s3://{BUCKET}/{path}")

# --- load Silver ---
order_items = read("silver/order_items")
order_opts  = read("silver/order_items_options")

# filter only in daily mode
if not full_refresh:
    order_items = order_items.filter(F.col("order_date") == F.lit(process_date))

# # --- Load Silver and filter to PROCESS_DATE ---
# order_items = read("silver/order_items").filter(F.col("order_date") == F.lit(PROCESS_DATE))
# order_opts  = read("silver/order_items_options")

# --- Options totals per line ---
order_opts_total_by_order_lineitem = (order_opts
    .groupBy("order_id","lineitem_id")
    .agg(F.sum(F.col("option_price") * F.col("option_quantity")).alias("options_total"))
)

# --- Enrich lines with options + compute line_total ---
order_items_with_options_totals = (order_items.join(order_opts_total_by_order_lineitem, ["order_id","lineitem_id"], "left")
    .withColumn("options_total", F.coalesce(F.col("options_total"), F.lit(0.0)))
    .withColumn("line_total", (F.col("order_total") + F.col("options_total")).cast("decimal(14,2)"))
)

# --- Roll up to ORDER grain so counts & loyalty are correct ---
# --- In theory, the loyalty setting would be 100% consistent at the order level.
# --- But in reality, fringe use-cases can cause (a) line-item(s) in the order 
# --- to have a False value for is_loyalty even though the customer is a loyalty
# --- member.  Code defensively and consider this a loyalty customer if ANY of the
# --- line items in the order register True for is_loyalty.
# --- We want to include it within the aggregation,
orders = (order_items_with_options_totals
    .groupBy("restaurant_id","order_date","order_id")
    .agg(
        F.sum("line_total").cast("decimal(14,2)").alias("order_gross"),
        F.sum("order_total").cast("decimal(14,2)").alias("order_item_sales"),
        F.sum("options_total").cast("decimal(14,2)").alias("order_option_sales"),
        F.sum("item_quantity").alias("order_items_count"),
        F.max(F.when(F.col("is_loyalty")==True, F.lit(1)).otherwise(F.lit(0))).alias("is_loyalty_order_int") # Any items in the order indicate "is_loyalty" True?
    )
    .withColumn("is_loyalty_order", (F.col("is_loyalty_order_int") == 1))
    .drop("is_loyalty_order_int")
)

# --- Final per-day, per-store aggregate ---
daily = (orders
    .groupBy("restaurant_id","order_date")
    .agg(
        F.sum("order_item_sales").cast("decimal(14,2)").alias("item_sales"),
        F.sum("order_option_sales").cast("decimal(14,2)").alias("option_sales"),
        F.sum("order_gross").cast("decimal(14,2)").alias("gross_sales_total"),
        F.countDistinct("order_id").alias("orders_count"),
        F.sum("order_items_count").alias("items_count"),
        F.sum(F.when(F.col("is_loyalty_order"), F.col("order_gross")).otherwise(F.lit(0.0))).cast("decimal(14,2)").alias("loyalty_sales"),
        F.sum(F.when(F.col("is_loyalty_order"), F.lit(1)).otherwise(F.lit(0))).alias("loyalty_orders")
    )
    .withColumn("non_loyalty_sales", (F.col("gross_sales_total") - F.col("loyalty_sales")).cast("decimal(14,2)"))
    .withColumn("non_loyalty_orders", (F.col("orders_count") - F.col("loyalty_orders")))
)

# --- Write partition for PROCESS_DATE ---
(daily.write.mode("overwrite")
 .option("compression","snappy")
 .partitionBy("order_date")
 .parquet(f"s3://{BUCKET}/gold/daily_sales_by_store/"))
 
# ---------- validation ----------
gold_path = f"s3://{BUCKET}/gold/daily_sales_by_store/"
g = spark.read.parquet(gold_path)

assert_nonempty(g, "Gold daily_sales_by_store is empty after write")
assert_has_columns(g, ["order_date"])
# Avoid null IDs blowing up dashboards
if "item_name" in g.columns:
    assert_no_nulls(g, ["order_date", "item_name"])


print("G1 complete.", "Mode=FULL" if full_refresh else f"PROCESS_DATE={process_date}")

