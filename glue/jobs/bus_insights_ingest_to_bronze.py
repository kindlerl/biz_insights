# glue_bronze_incremental.py (PySpark / Glue 4.0)

import re, sys, os
from datetime import datetime, timedelta, timezone
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

args = getResolvedOptions(sys.argv, [
    "BUCKET", "DB_HOST", "DB_NAME", "DB_USER", "DB_PASS"
] + (["PROCESS_DATE"] if "--PROCESS_DATE" in sys.argv else []))

BUCKET   = args["BUCKET"]
DB_HOST  = args["DB_HOST"]
DB_NAME  = args["DB_NAME"]
DB_USER  = args["DB_USER"]
DB_PASS  = args["DB_PASS"]

process_date = args.get("PROCESS_DATE")
if not process_date:
    process_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

if not re.match(r"^\d{4}-\d{2}-\d{2}$", process_date):
    raise ValueError(f"Bad PROCESS_DATE: {process_date}")

spark = SparkSession.builder.appName("bronze-ingest-incremental").getOrCreate()

jdbc_url = f"jdbc:sqlserver://{DB_HOST}:1433;databaseName={DB_NAME};encrypt=true;trustServerCertificate=true"

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

def read_jdbc(sql: str):
    return (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("user", DB_USER)
            .option("password", DB_PASS)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("fetchsize", 10000)
            .option("query", sql)
            .load())

def write_partition(df, name: str):
    # Idempotent write of a SINGLE partition
    out_path = f"s3://{BUCKET}/bronze/{name}/ingestion_date={process_date}/"
    (df.withColumn("ingestion_date", F.lit(process_date).cast("string"))
       .write.mode("overwrite")  # overwrites only this partition path
       .option("compression", "snappy")
       .parquet(out_path))
    print(f"Wrote {name} to {out_path} (rows={df.count()})")

# -------- order_items (incremental by business date) --------
order_items_sql = f"""
SELECT
  app_name,
  restaurant_id,
  creation_time_utc,
  order_id,
  user_id,
  printed_card_number,
  is_loyalty,
  currency,
  lineitem_id,
  item_category,
  item_name,
  item_price,
  item_quantity
FROM dbo.order_items
WHERE CAST(creation_time_utc AS date) = '{process_date}'
"""

df_items = read_jdbc(order_items_sql)

# (Optional) dedupe within the day by a stable key
if "lineitem_id" in df_items.columns:
    df_items = df_items.dropDuplicates(["lineitem_id"])
elif set(["order_id", "item_name"]).issubset(df_items.columns):
    df_items = df_items.dropDuplicates(["order_id", "item_name"])
    
# Check to see if there were any orders found and set a flag if there were.
wrote_any = False
cnt = df_items.limit(1).count()
if cnt == 0:
    print(f"[Bronze] No df_items source rows for ingestion_date={process_date}. No-op (success).")
else:
    write_partition(df_items, "order_items")
    wrote_any = True

# -------- order_items_options (only options for that day’s orders) --------
order_items_options_sql = f"""
SELECT
  o.order_id,
  o.lineitem_id,
  o.option_name,
  o.option_price,
  o.option_quantity
FROM dbo.order_items_options AS o
WHERE o.order_id IN (
  SELECT i.order_id
  FROM dbo.order_items AS i
  WHERE CAST(i.creation_time_utc AS date) = '{process_date}'
)
"""
df_opts = read_jdbc(order_items_options_sql)

cnt = df_opts.limit(1).count()
if cnt == 0:
    print(f"[Bronze] No df_opts source rows for ingestion_date={process_date}. No-op (success).")
else:
    write_partition(df_opts, "order_items_options")
    wrote_any = True

# -------- date_dim (single row for the day) --------
# If you already have a date table in SQL Server, swap this for a SELECT … WHERE date=process_date.
dt = datetime.strptime(process_date, "%Y-%m-%d")
df_date = spark.createDataFrame([{
    "date": process_date,
    "year": dt.year,
    "month": dt.month,
    "day": dt.day,
    "week": int(dt.strftime("%V")),            # ISO week
    "is_weekend": dt.weekday() >= 5,
    "is_holiday": False                        # you can enrich later
}])

cnt = df_date.limit(1).count()
if cnt == 0:
    print(f"[Bronze] No df_date source rows for ingestion_date={process_date}. No-op (success).")
else:
    write_partition(df_date, "date_dim")
    wrote_any = True

if wrote_any:
    bronze_items = (spark.read
        .option("basePath", f"s3://{BUCKET}/bronze/order_items/")
        .parquet(f"s3://{BUCKET}/bronze/order_items/ingestion_date={process_date}"))
    assert_has_columns(bronze_items, ["order_id","lineitem_id","item_price","item_quantity","ingestion_date"])

print(f"Bronze incremental ingest complete for {process_date}.")

