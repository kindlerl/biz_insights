# glue/jobs/gold_g3_customer_facts.py

import sys
import datetime as dt
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F, Window

# ---------- args ----------
# req = ['BUCKET']
# opt = ['SNAPSHOT_DATE','FULL_REFRESH']
# present = req + [k for k in opt if f'--{k}' in sys.argv]
# args = getResolvedOptions(sys.argv, present)

# BUCKET        = args['BUCKET'].strip().strip('"').strip("'")
# SNAPSHOT_DATE = args.get('SNAPSHOT_DATE') or (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
# FULL_REFRESH  = args.get('FULL_REFRESH', 'false').lower() == 'true'

# Let's get the SNAPSHOT_DATE from Silver, if it exists and it is not passed as an argument
args = getResolvedOptions(sys.argv, ['BUCKET'] + (['SNAPSHOT_DATE'] if '--SNAPSHOT_DATE' in sys.argv else []))
BUCKET = args['BUCKET']
SNAPSHOT_DATE = args.get('SNAPSHOT_DATE')

# ---------- spark ----------
spark = (SparkSession.builder.appName("gold-customer-facts").getOrCreate())
spark.conf.set("spark.sql.session.timeZone","UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

def yesterday_utc():
    return (dt.datetime.now(timezone.utc) - dt.timedelta(days=1)).date()

if not SNAPSHOT_DATE:
    # Try to infer from Silver first
    silver = spark.read.parquet(f"s3://{BUCKET}/silver/order_items/").select('order_date')
    max_row = silver.agg(F.max('order_date').alias('d')).first()
    if max_row and max_row['d'] is not None:
        snap = max_row['d']                 # Python date
    else:
        snap = yesterday_utc()              # fallback if Silver empty
    SNAPSHOT_DATE = snap.strftime('%Y-%m-%d')

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

read = lambda p: spark.read.parquet(f"s3://{BUCKET}/{p}")

# ---------- load silver & filter to <= snapshot_date ----------
order_items = (read("silver/order_items")
         .select("user_id","order_id","lineitem_id","order_date",
                 "item_quantity","order_total","is_loyalty")
         .filter(F.col("order_date") <= F.lit(SNAPSHOT_DATE))
)
order_options  = read("silver/order_items_options") \
          .select("order_id","lineitem_id","option_price","option_quantity")

# ---------- per-line options totals ----------
opts_line = (order_options
  .groupBy("order_id","lineitem_id")
  .agg(F.sum(F.col("option_price")*F.col("option_quantity")).alias("options_total"))
)

# ---------- get rid of any null user_id rows before rolling up to orders
order_items = order_items.filter(F.col("user_id").isNotNull())

# ---------- enrich lines, then roll up to ORDER grain (per user) ----------
order_items_with_totals = (order_items
  .join(opts_line, ["order_id","lineitem_id"], "left")
  .withColumn("options_total", F.coalesce(F.col("options_total"), F.lit(0.0)))
  .withColumn("line_total", (F.col("order_total") + F.col("options_total")).cast("decimal(18,4)"))
)

# It's possible that one or more lineitems may not have the is_loyalty flag set even though the
# customer is a loyalty member (for multiple reasons).  If ANY of the lineitems is flagged for
# is_loyalty, consider the entire order to be a loyalty order.
orders = (order_items_with_totals
  .groupBy("user_id","order_date","order_id")
  .agg(
    F.sum("line_total").cast("decimal(18,4)").alias("order_gross"),
    F.sum("item_quantity").alias("order_items_count"),
    F.max(F.col("is_loyalty").cast("int")).alias("is_loyalty_order_int") # Look for ANY loyalty flag on any line of the order.
  )
  .withColumn("is_loyalty_order", F.col("is_loyalty_order_int")==1)
  .drop("is_loyalty_order_int")
)

# ---------- lifetime aggregates up to snapshot ----------
lifetime = (orders.groupBy("user_id")
  .agg(
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
    F.countDistinct("order_id").alias("lifetime_orders"),
    F.sum("order_gross").cast("decimal(18,4)").alias("lifetime_gross_sales"),
    F.sum(F.when(F.col("is_loyalty_order"), 1).otherwise(0)).alias("loyalty_orders"),
    F.sum(F.when(F.col("is_loyalty_order"), 0).otherwise(1)).alias("non_loyalty_orders")
  )
  .withColumn("avg_order_value",
      F.when(F.col("lifetime_orders")>0,
             (F.col("lifetime_gross_sales")/F.col("lifetime_orders")).cast("decimal(18,4)"))
       .otherwise(F.lit(None)))
  .withColumn("days_since_last_order",
      F.datediff(F.lit(SNAPSHOT_DATE), F.col("last_order_date")))
)

# ---------- period windows (last 90d, prev 90d relative to snapshot) ----------
snapshot_date      = dt.date.fromisoformat(SNAPSHOT_DATE)
last90_start_date  = (snapshot_date - dt.timedelta(days=89)).isoformat()   # inclusive
prev90_start_date  = (snapshot_date - dt.timedelta(days=179)).isoformat()
prev90_end_date    = (snapshot_date - dt.timedelta(days=90)).isoformat()

orders_last90 = orders.filter((F.col("order_date") >= F.lit(last90_start_date)) &
                              (F.col("order_date") <= F.lit(snapshot_date)))
orders_prev90 = orders.filter((F.col("order_date") >= F.lit(prev90_start_date)) &
                              (F.col("order_date") <= F.lit(prev90_end_date)))

last90_order_count_and_sales = (orders_last90.groupBy("user_id")
          .agg(F.countDistinct("order_id").alias("last_90d_orders_count"),
               F.sum("order_gross").cast("decimal(18,4)").alias("last_90d_sales")))

prev90_order_couint_and_sales = (orders_prev90.groupBy("user_id")
          .agg(F.sum("order_gross").cast("decimal(18,4)").alias("prev_90d_sales")))

# ---------- inter-order gaps (avg days between orders) ----------
window_spec = Window.partitionBy("user_id").orderBy("order_date")
gaps = (orders
  .withColumn("prev_date", F.lag("order_date").over(window_spec))
  .withColumn("gap_days", F.when(F.col("prev_date").isNotNull(),
                                 F.datediff(F.col("order_date"), F.col("prev_date"))))
  .groupBy("user_id").agg(F.avg("gap_days").alias("avg_interorder_gap_days"))
)

# ---------- assemble customer base features ----------
base = (lifetime
  .join(last90_order_count_and_sales, "user_id", "left")
  .join(prev90_order_couint_and_sales, "user_id", "left")
  .join(gaps,   "user_id", "left")
  .fillna({"last_90d_orders_count":0, "last_90d_sales":0.0, "prev_90d_sales":0.0})
  .withColumn("delta_90d_pct",
      F.when(F.col("prev_90d_sales") > 0,
             (F.col("last_90d_sales") - F.col("prev_90d_sales")) / F.col("prev_90d_sales"))
      .otherwise(F.lit(None)))
)
# base = (customers
#         .join(last90, "user_id", "left")
#         .join(prev90, "user_id", "left")
#         .na.fill({"last_90d_orders": 0, "last_90d_sales": 0.0, "prev_90d_sales": 0.0}))

# ---------- RFM scoring (quintiles over snapshot cohort) ----------
# R: smaller days_since_last_order -> higher score
# F: higher last_90d_orders_count -> higher score
# M: higher last_90d_sales  -> higher score
thresholds = {}
for col, probabilities in [
    ("days_since_last_order", [0.2,0.4,0.6,0.8]),
    ("last_90d_orders_count", [0.2,0.4,0.6,0.8]),
    ("last_90d_sales",        [0.2,0.4,0.6,0.8])
]:
    # none values: replace with large/small sentinels for quantiles
    tmp = base.select(F.col(col)).na.fill({col: 999999 if col=="days_since_last_order" else 0})
    thresholds[col] = tmp.stat.approxQuantile(col, probabilities, 0.01)

recency_qantile   = thresholds["days_since_last_order"]   # lower is better
frequency_qantile = thresholds["last_90d_orders_count"]
monetary_qantile  = thresholds["last_90d_sales"]

def bucket_desc(col, q, invert=False):
    c = F.col(col)
    if invert:  # smaller -> higher score
        return (F.when(c <= F.lit(q[0]), 5)
                 .when(c <= F.lit(q[1]), 4)
                 .when(c <= F.lit(q[2]), 3)
                 .when(c <= F.lit(q[3]), 2)
                 .otherwise(1))
    else:
        return (F.when(c <= F.lit(q[0]), 1)
                 .when(c <= F.lit(q[1]), 2)
                 .when(c <= F.lit(q[2]), 3)
                 .when(c <= F.lit(q[3]), 4)
                 .otherwise(5))

scored = (base
  .withColumn("recency_score", bucket_desc("days_since_last_order", recency_qantile, invert=True))
  .withColumn("frequency_score", bucket_desc("last_90d_orders_count", frequency_qantile, invert=False))
  .withColumn("monetary_score", bucket_desc("last_90d_sales",  monetary_qantile, invert=False))
)

# simple segment labels
seg = (scored
  .withColumn("clv_band",
      F.when(F.col("lifetime_gross_sales") >= F.expr("percentile_approx(lifetime_gross_sales, 0.8) OVER ()"), F.lit("High"))
       .when(F.col("lifetime_gross_sales") >= F.expr("percentile_approx(lifetime_gross_sales, 0.2) OVER ()"), F.lit("Medium"))
       .otherwise(F.lit("Low"))
  )
  .withColumn("segment_label",
      F.when( (F.col("recency_score")>=4) & (F.col("frequency_score")>=4) & (F.col("monetary_score")>=4), F.lit("VIP"))
       .when( F.col("recency_score")<=2, F.lit("Churn-risk"))
       .when( (F.col("frequency_score")>=4) & (F.col("monetary_score")>=4), F.lit("High-Value"))
       .otherwise(F.lit("Standard"))
  )
)

# final select & types
final = (seg.select(
    F.lit(SNAPSHOT_DATE).cast("date").alias("snapshot_date"),
    "user_id",
    "first_order_date","last_order_date",
    "lifetime_orders",
    F.col("lifetime_gross_sales").cast("decimal(14,2)").alias("lifetime_gross_sales"),
    F.col("avg_order_value").cast("decimal(14,2)").alias("avg_order_value"),
    "loyalty_orders","non_loyalty_orders",
    "last_90d_orders_count",
    F.col("last_90d_sales").cast("decimal(14,2)").alias("last_90d_sales"),
    F.col("prev_90d_sales").cast("decimal(14,2)").alias("prev_90d_sales"),
    F.col("delta_90d_pct").cast("double").alias("delta_90d_pct"),
    F.col("days_since_last_order").cast("int").alias("days_since_last_order"),
    F.col("avg_interorder_gap_days").cast("double").alias("avg_interorder_gap_days"),
    F.col("recency_score").cast("int"), F.col("frequency_score").cast("int"), F.col("monetary_score").cast("int"),
    "clv_band","segment_label"
))

# ---------- write (partition by snapshot_date) ----------
(final
    .repartition("snapshot_date") 
    .write.mode("overwrite")
    .option("compression","snappy")
    .partitionBy("snapshot_date")
    .parquet(f"s3://{BUCKET}/gold/customer_facts/"))

# ---------- validation ----------
gold_path = f"s3://{BUCKET}/gold/customer_facts/"
g = spark.read.parquet(gold_path)

assert_nonempty(g, "Gold customer_facts is empty after write")
assert_has_columns(g, ["snapshot_date", "user_id"])

from pyspark.sql import functions as F
mx = g.agg(F.max("snapshot_date").alias("d")).first()["d"]
assert_true(mx is not None, "customer_facts has no snapshot_date")
assert_true(str(mx) == SNAPSHOT_DATE, f"snapshot_date mismatch: {mx} != {SNAPSHOT_DATE}")

# reasonable null-guards
assert_no_nulls(g, ["snapshot_date", "user_id"])

print(f"G3 complete for snapshot_date={SNAPSHOT_DATE}")

