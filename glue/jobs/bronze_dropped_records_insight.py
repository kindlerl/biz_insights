import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

# ----------------- params -----------------
need_db = False
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET','S3_PREFIX','DB_USER','DB_PASS','RDS_HOST','DB_NAME'])
    need_db = True
except:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET','S3_PREFIX'])

def clean(s): return s.strip().strip('"').strip("'")
S3_BUCKET = clean(args['S3_BUCKET'])
S3_PREFIX = clean(args['S3_PREFIX'])

if need_db:
    DB_USER = clean(args['DB_USER']); DB_PASS = args['DB_PASS']
    RDS_HOST= clean(args['RDS_HOST']); DB_NAME = clean(args['DB_NAME'])
    jdbc_url = f"jdbc:sqlserver://{RDS_HOST}:1433;databaseName={DB_NAME};encrypt=true;trustServerCertificate=true"

spark = SparkSession.builder.appName("audit-seed-deltas").getOrCreate()

# --------------- helpers (same as seed) ---------------
def strip_urls(col): return F.trim(F.regexp_replace(col, r'https?://\S+', ''))
def cap(colname, n):
    c = F.col(colname) if isinstance(colname,str) else colname
    return F.when(F.length(c) > n, F.expr(f"substring({colname},1,{n})")).otherwise(c)
def norm_bool(col):
    c = F.col(col) if isinstance(col,str) else col
    return (F.when(F.upper(c).isin("TRUE","T","YES","Y","1"), True)
             .when(F.upper(c).isin("FALSE","F","NO","N","0"), False)
             .otherwise(c.cast("boolean")))
def safe_decimal(col):
    x = F.col(col)
    x = F.regexp_replace(x, r'[\u2012\u2013\u2014\u2212]', '-')          # unicode minus → -
    x = F.regexp_replace(x, r'[\$,]', '')                                 # $, commas
    x = F.trim(x)
    x = F.regexp_replace(x, r'^\((.*)\)$', r'-\1')                        # (7.00) → -7.00
    x = F.when(x.rlike(r'^\s*$|^-$|^\.$|^-\.$'), None).otherwise(x)       # empties/dots/dashes → NULL
    x = F.when(x.rlike(r'^-?\d+(\.\d+)?$'), x).otherwise(None)            # allow only numeric forms
    return x.cast("decimal(10,2)")

def parse_date_multi(col, patterns):
    return F.coalesce(*(F.to_date(col,p) for p in patterns))

def log(label, df):
    c = df.count()
    print(f"{label}: {c}")
    return c

# ----------------- ITEMS -----------------
items_raw = spark.read.option("header", True).csv(f"s3://{S3_BUCKET}/{S3_PREFIX}/order_items.csv")
log("items_raw", items_raw)

items = (items_raw
  .withColumn("creation_time_utc", F.to_timestamp("CREATION_TIME_UTC"))
  .withColumn("printed_card_number", F.regexp_replace("PRINTED_CARD_NUMBER", r'\d(?=\d{4})', 'X'))
  .withColumn("is_loyalty", norm_bool("IS_LOYALTY"))
  .withColumn("item_price", safe_decimal("ITEM_PRICE"))
  .withColumn("item_quantity", F.col("ITEM_QUANTITY").cast("int"))
  .withColumn("item_category", strip_urls("ITEM_CATEGORY"))
  .withColumn("item_name", F.col("ITEM_NAME"))
  .selectExpr(
    "APP_NAME as app_name",
    "RESTAURANT_ID as restaurant_id",
    "creation_time_utc",
    "ORDER_ID as order_id",
    "USER_ID as user_id",
    "printed_card_number",
    "CURRENCY as currency",
    "LINEITEM_ID as lineitem_id",
    "item_category",
    "item_name",
    "item_price",
    "item_quantity",
    "is_loyalty"
  )
)

log("items_after_casts", items)

items_dedup = items.dropDuplicates(["order_id","lineitem_id"])
print("items_dupes_removed:", items.count() - items_dedup.count())

invalid_items = (
    F.col("creation_time_utc").isNull() |
    F.col("order_id").isNull() | F.col("lineitem_id").isNull() |
    (F.length("item_category") > 64) |
    (F.length("item_name") > 128) |
    F.col("item_price").isNull()
)

items_rejected = (items_dedup.where(invalid_items).withColumn(
    "reason",
    F.when(F.col("creation_time_utc").isNull(), "NULL_creation_time")
     .when(F.col("order_id").isNull() | F.col("lineitem_id").isNull(), "NULL_key")
     .when(F.length("item_category") > 64, "item_category_too_long")
     .when(F.length("item_name") > 128, "item_name_too_long")
     .when(F.col("item_price").isNull(), "bad_price")
))

log("items_rejected", items_rejected)
items_rejected.groupBy("reason").count().show(truncate=False)

items_clean = (items_dedup.where(~invalid_items)
    .withColumn("item_category", cap("item_category", 64))
    .withColumn("item_name",     cap("item_name", 128))
)
log("items_clean", items_clean)

# ----------------- OPTIONS -----------------
opts_raw = spark.read.option("header", True).csv(f"s3://{S3_BUCKET}/{S3_PREFIX}/order_item_options.csv")
log("opts_raw", opts_raw)

opts = (opts_raw
    .withColumn("order_id",    F.col("ORDER_ID"))
    .withColumn("lineitem_id", F.col("LINEITEM_ID"))
    .withColumn("option_group_name", cap("OPTION_GROUP_NAME", 64))
    .withColumn("option_name",       cap("OPTION_NAME", 128))
    .withColumn("option_price",   safe_decimal("OPTION_PRICE"))
    .withColumn("option_quantity", F.col("OPTION_QUANTITY").cast("int"))
    .select("order_id","lineitem_id","option_group_name","option_name","option_price","option_quantity")
)

opts_clean = (opts
    .na.drop(subset=["order_id","lineitem_id","option_name"])
    .filter(F.col("option_price").isNotNull())
    .dropDuplicates(["order_id","lineitem_id","option_name"])
)
log("opts_clean", opts_clean)

if need_db:
    existing_keys = (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", DB_USER).option("password", DB_PASS)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", "(SELECT order_id, lineitem_id FROM dbo.order_items) k")
        .load())
    opts_parented = opts_clean.join(existing_keys, ["order_id","lineitem_id"], "inner")
    opts_orphans  = opts_clean.join(existing_keys, ["order_id","lineitem_id"], "left_anti")
    log("opts_parented", opts_parented)
    log("opts_orphans",  opts_orphans)
else:
    print("DB params not provided → skipping orphans metric (opts_parented/opts_orphans).")

# ----------------- DATE DIM -----------------
dt_raw = spark.read.option("header", True).csv(f"s3://{S3_BUCKET}/{S3_PREFIX}/date_dim.csv")
log("date_dim_raw", dt_raw)

dt = dt_raw.withColumn("date_key_str", F.trim(F.col("date_key")))
date_key = parse_date_multi(F.col("date_key_str"), ["M/d/yy","dd-MM-yyyy","M/d/yyyy","MM-dd-yyyy"])
dt_parsed = (dt
    .withColumn("date_key", date_key)
    .withColumn("is_weekend", norm_bool("is_weekend"))
    .withColumn("is_holiday", norm_bool("is_holiday"))
)
dt_bad  = dt_parsed.filter(F.col("date_key").isNull())
dt_good = dt_parsed.filter(F.col("date_key").isNotNull())
log("date_dim_bad", dt_bad)
log("date_dim_good", dt_good)

print("Audit complete.")

