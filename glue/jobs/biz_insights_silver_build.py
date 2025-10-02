import sys, os
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, BooleanType
from datetime import datetime, timedelta, timezone


# ---- args ----
want = ['S3_BUCKET']
opt  = ['RUN_DATE', 'CLEANUP', 'FULL_RELOAD']   # RUN_DATE = ingestion_date (YYYY-MM-DD); FULL_RELOAD=true to process all data
args = getResolvedOptions(sys.argv, want + [a for a in opt if f'--{a}' in sys.argv])

BUCKET = args['S3_BUCKET'].strip().strip('"').strip("'")
RUN_DATE = args.get('RUN_DATE')
CLEANUP = args.get('CLEANUP', 'false').lower() == 'true'
FULL_RELOAD = args.get('FULL_RELOAD', 'false').lower() == 'true'

spark = SparkSession.builder.appName("silver-build").getOrCreate()

if CLEANUP:
    # --- order_items: keep the newest by ingestion_date per lineitem_id ---
    df = spark.read.parquet(f"s3://{BUCKET}/silver/order_items/")
    w  = Window.partitionBy("lineitem_id").orderBy(F.col("ingestion_date").desc_nulls_last())
    clean = (df.withColumn("rn", F.row_number().over(w))
               .where("rn = 1")
               .drop("rn"))
    (clean.write.mode("overwrite")
          .option("compression","snappy")
          .parquet(f"s3://{BUCKET}/silver/order_items/"))

    # --- order_items_options: dedupe too (adjust keys if needed) ---
    df2 = spark.read.parquet(f"s3://{BUCKET}/silver/order_items_options/")
    w2  = Window.partitionBy("order_id","lineitem_id","option_name")\
                .orderBy(F.col("ingestion_date").desc_nulls_last())
    clean2 = (df2.withColumn("rn", F.row_number().over(w2))
                 .where("rn = 1")
                 .drop("rn"))
    (clean2.write.mode("overwrite")
          .option("compression","snappy")
          .parquet(f"s3://{BUCKET}/silver/order_items_options/"))

    print("One-time Silver cleanup complete.")
    sys.exit(0)

if not RUN_DATE:
    RUN_DATE = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')  # default: yesterday UTC

# ---------- helpers ------------
def read_bronze(name: str):
    base = f"s3://{BUCKET}/bronze/{name}/"
    path = f"{base}ingestion_date={RUN_DATE}"
    if FULL_RELOAD:
        return spark.read.parquet(base)  # all partitions
    else:
        try:
            df = spark.read.option("basePath", base).parquet(path)
            # cheap existence check
            if df.limit(1).count() == 0:
                print(f"[Silver] Bronze {name} exists but is empty for {RUN_DATE}")
                return None
            return df
        except Exception as e:
            print(f"[Silver] No Bronze partition for {name} at {path} ({type(e).__name__}).")
            return None

wrote_any = False

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

# ---------- order_items ----------
items_src = read_bronze("order_items")

if items_src is not None:
    items = (items_src
        .withColumn("item_price", F.col("item_price").cast("decimal(10,2)"))
        .withColumn("item_quantity", F.col("item_quantity").cast("int"))
        .withColumn("is_loyalty", F.col("is_loyalty").cast("boolean"))
        .withColumn("creation_time_utc", F.col("creation_time_utc").cast("timestamp"))
        .withColumn("order_date", F.to_date("creation_time_utc"))
        .withColumn("order_total", (F.col("item_price") * F.col("item_quantity")).cast("decimal(12,2)"))
        .withColumn("ingestion_date", F.to_date("ingestion_date"))  # from partition/column
    )
    
    # Dedupe: keep the newest by ingestion_date per logical line item
    dedupe_key = F.sha2(F.concat_ws('|',
        items['order_id'].cast('string'),
        F.coalesce(items['lineitem_id'].cast('string'), F.lit('∅')),
        items['item_category'].cast('string'),
        items['item_name'].cast('string'),
        items['item_price'].cast('string'),
        items['item_quantity'].cast('string')
    ), 256)
    
    w_items = Window.partitionBy(dedupe_key).orderBy(F.col("ingestion_date").desc_nulls_last())
    items_out = (items.withColumn("dk", dedupe_key)
                      .withColumn("rn", F.row_number().over(w_items))
                      .where("rn = 1")
                      .drop("rn", "dk"))
    
    (items_out.write
        .mode("overwrite" if FULL_RELOAD else "append")
        .option("compression","snappy")
        .parquet(f"s3://{BUCKET}/silver/order_items/"))
    
# ---------- order_items_options ----------
opts_src = read_bronze("order_items_options")

if opts_src is not None:
    opts = (opts_src
        .withColumn("option_price", F.col("option_price").cast("decimal(10,2)"))
        .withColumn("option_quantity", F.col("option_quantity").cast("int"))
        .withColumn("ingestion_date", F.to_date("ingestion_date"))
    )
    
    dedupe_key = F.sha2(F.concat_ws('|',
        opts['order_id'].cast('string'),
        F.coalesce(opts['lineitem_id'].cast('string'), F.lit('∅')),
        opts['option_name'].cast('string'),
        opts['option_price'].cast('string'),
        opts['option_quantity'].cast('string')
    ), 256)
    
    w_opts = Window.partitionBy(dedupe_key).orderBy(F.col("ingestion_date").desc_nulls_last())
    
    opts_out = (opts.withColumn("dk", dedupe_key)
                    .withColumn("rn", F.row_number().over(w_opts))
                    .where("rn = 1")
                    .drop("rn", "dk"))
                    
    (opts_out.write
        .mode("overwrite" if FULL_RELOAD else "append")
        .option("compression","snappy")
        .parquet(f"s3://{BUCKET}/silver/order_items_options/"))

# # ---------- date_dim ----------
def build_date_df(run_date: str):
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    schema = StructType([
        StructField("date_key",       DateType(),   False),
        StructField("year",           IntegerType(),False),
        StructField("month",          IntegerType(),False),
        StructField("day",            IntegerType(),False),
        StructField("week",           IntegerType(),False),
        StructField("day_of_week",    StringType(), False),
        StructField("is_weekend",     BooleanType(),False),
        StructField("is_holiday",     BooleanType(),False),
        StructField("holiday_name",   StringType(),  True),
        StructField("ingestion_date", DateType(),   False)
    ])
    rows = [{
        "date_key": dt.date(),
        "year": dt.year,
        "month": dt.month,
        "day": dt.day,
        "week": int(dt.strftime("%V")),           # ISO week number
        "day_of_week": dt.strftime("%A"),
        "is_weekend": dt.weekday() >= 5,
        "is_holiday": False,
        "holiday_name": None,
        "ingestion_date": dt.date()
    }]
    return spark.createDataFrame(rows, schema)

if wrote_any or FULL_RELOAD:
    date_new = build_date_df(RUN_DATE)
    out_path = f"s3://{BUCKET}/silver/date_dim/"
    
    # Write-by-day overwrite to avoid duping this date
    try:
        existing = spark.read.parquet(out_path)
        final = (existing
                 .filter(F.col("date_key") != F.to_date(F.lit(RUN_DATE)))
                 .unionByName(date_new))
        (final.write
            .mode("overwrite")
            .option("compression","snappy")
            .parquet(out_path))
    except Exception:
        # first-time write path
        (date_new.write
            .mode("append")
            .option("compression","snappy")
            .parquet(out_path))


print(f"Silver build complete for RUN_DATE={RUN_DATE}, FULL_RELOAD={FULL_RELOAD}.")

