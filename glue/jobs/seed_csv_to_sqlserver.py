import sys, time
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

# ---------------- Params ----------------
args = getResolvedOptions(sys.argv, [
    'DB_USER','DB_PASS','RDS_HOST','S3_BUCKET','S3_PREFIX','DB_NAME','REJECT_PREFIX'
])

def clean(s): return s.strip().strip("'").strip('"')  # trim accidental spaces/quotes

DB_NAME   = clean(args['DB_NAME'])
DB_USER   = clean(args['DB_USER'])
DB_PASS   = args['DB_PASS']           # don't strip passwords
RDS_HOST  = clean(args['RDS_HOST'])
S3_BUCKET = clean(args['S3_BUCKET'])
S3_PREFIX = clean(args['S3_PREFIX'])
REJECT_PREFIX = clean(args['REJECT_PREFIX']) if 'REJECT_PREFIX' in args else "rejected"

RUN_TS = time.strftime("%Y%m%d-%H%M%S")

spark = SparkSession.builder.appName("seed-to-sqlserver").getOrCreate()

jdbc_url  = f"jdbc:sqlserver://{RDS_HOST}:1433;databaseName={DB_NAME};encrypt=true;trustServerCertificate=true;loginTimeout=15"
jdbc_opts = {"url": jdbc_url, "user": DB_USER, "password": DB_PASS,
             "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "batchsize": "2000"}

# ---------------- Helpers ----------------
def strip_urls(col):
    return F.trim(F.regexp_replace(col, r'https?://\S+', ''))

def cap(colname, n):
    c = F.col(colname) if isinstance(colname, str) else colname
    return F.when(F.length(c) > n, F.expr(f"substring({colname},1,{n})")).otherwise(c)

def norm_bool(col):
    c = F.col(col) if isinstance(col, str) else col
    return (F.when(F.upper(c).isin("TRUE","T","YES","Y","1"), F.lit(True))
             .when(F.upper(c).isin("FALSE","F","NO","N","0"), F.lit(False))
             .otherwise(c.cast("boolean")))

def safe_decimal(col):
    x = F.col(col)
    # normalize unicode dashes to ASCII minus
    x = F.regexp_replace(x, r'[\u2012\u2013\u2014\u2212]', '-')
    # remove currency symbols/commas
    x = F.regexp_replace(x, r'[\$,]', '')
    x = F.trim(x)
    # parentheses negatives: (7.00) -> -7.00
    x = F.regexp_replace(x, r'^\((.*)\)$', r'-\1')
    # treat '', '-', '.', '-.' as NULL
    x = F.when(x.rlike(r'^\s*$|^-$|^\.$|^-\.$'), None).otherwise(x)
    # only allow -?\d+(\.\d+)?; else NULL
    x = F.when(x.rlike(r'^-?\d+(\.\d+)?$'), x).otherwise(None)
    return x.cast("decimal(10,2)")

def mask_last4(col):
    return F.regexp_replace(col, r'\d(?=\d{4})', 'X')

def parse_date_multi(col, patterns):
    return F.coalesce(*(F.to_date(col, p) for p in patterns))

# ---------------- One-time reset (DEV seed) ----------------
j = spark._jvm
DriverManager = j.java.sql.DriverManager
def exec_sql(db, sql):
    url = f"jdbc:sqlserver://{RDS_HOST}:1433;databaseName={db};encrypt=true;trustServerCertificate=true"
    c = DriverManager.getConnection(url, DB_USER, DB_PASS)
    try:
        s = c.createStatement(); s.execute(sql); s.close()
    finally:
        c.close()

# child → parent → dim
exec_sql(DB_NAME, "DELETE FROM dbo.order_items_options;")
exec_sql(DB_NAME, "DELETE FROM dbo.order_items;")
exec_sql(DB_NAME, "DELETE FROM dbo.date_dim;")

# ===========================================================
# ===============  order_items (PARENT)  ====================
# ===========================================================
items_raw = (spark.read
    .option("header", True)
    .csv(f"s3://{S3_BUCKET}/{S3_PREFIX}/order_items.csv"))

items = (items_raw
  .withColumn("creation_time_utc", F.to_timestamp("CREATION_TIME_UTC"))
  .withColumn("printed_card_number", mask_last4("PRINTED_CARD_NUMBER"))
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
  .dropDuplicates(["order_id","lineitem_id"])
)

invalid_items = (
    F.col("creation_time_utc").isNull() |
    F.col("order_id").isNull() | F.col("lineitem_id").isNull() |
    (F.length("item_category") > 64) |
    (F.length("item_name") > 128) |
    F.col("item_price").isNull()
)

items_rejected = (items.where(invalid_items).withColumn(
    "reason",
    F.when(F.col("creation_time_utc").isNull(), "NULL_creation_time")
     .when(F.col("order_id").isNull() | F.col("lineitem_id").isNull(), "NULL_key")
     .when(F.length("item_category") > 64, "item_category_too_long")
     .when(F.length("item_name") > 128, "item_name_too_long")
     .when(F.col("item_price").isNull(), "bad_price")
))

if items_rejected.take(1):
    (items_rejected.write.mode("overwrite")
        .csv(f"s3://{S3_BUCKET}/{REJECT_PREFIX}/order_items/{RUN_TS}/"))

items_clean = (items.where(~invalid_items)
    .withColumn("item_category", cap("item_category", 64))
    .withColumn("item_name",     cap("item_name", 128))
)

(items_clean.write.format("jdbc")
  .options(**jdbc_opts)
  .option("dbtable","dbo.order_items")
  .mode("append")
  .save())

# ===========================================================
# ==========  order_items_options (CHILD)  ==================
# ===========================================================
# CSV file is singular name per your dataset
opts_raw = (spark.read
    .option("header", True)
    .csv(f"s3://{S3_BUCKET}/{S3_PREFIX}/order_item_options.csv"))

opts = (opts_raw
    .withColumn("order_id",    F.col("ORDER_ID"))
    .withColumn("lineitem_id", F.col("LINEITEM_ID"))
    .withColumn("option_group_name", cap("OPTION_GROUP_NAME", 64))
    .withColumn("option_name",       cap("OPTION_NAME", 128))
    .withColumn("option_price",   safe_decimal("OPTION_PRICE"))
    .withColumn("option_quantity", F.col("OPTION_QUANTITY").cast("int"))
    .select("order_id","lineitem_id","option_group_name","option_name","option_price","option_quantity")
    .na.drop(subset=["order_id","lineitem_id","option_name"])
    .filter(F.col("option_price").isNotNull())
    .dropDuplicates(["order_id","lineitem_id","option_name"])
)

# Read the actual parent keys that made it into SQL Server
existing_keys = (spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", DB_USER).option("password", DB_PASS)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("dbtable", "(SELECT order_id, lineitem_id FROM dbo.order_items) k")
    .load())

# Split parented vs orphans (FK-safe insert)
opts_parented = opts.join(existing_keys, ["order_id","lineitem_id"], "inner")
opts_orphans  = opts.join(existing_keys, ["order_id","lineitem_id"], "left_anti")

if opts_orphans.take(1):
    (opts_orphans.withColumn("reason", F.lit("missing_parent"))
        .write.mode("overwrite")
        .csv(f"s3://{S3_BUCKET}/{REJECT_PREFIX}/order_items_options/{RUN_TS}/"))

(opts_parented.write.format("jdbc")
  .options(**jdbc_opts)
  .option("dbtable","dbo.order_items_options")  # PLURAL table name (matches your DDL)
  .mode("append")
  .save())

# ===========================================================
# ===================  date_dim (DIM)  ======================
# ===========================================================
# CSV is named date_time.csv in your dataset
dt_raw = (spark.read
    .option("header", True)
    .csv(f"s3://{S3_BUCKET}/{S3_PREFIX}/date_time.csv"))

dt = dt_raw.withColumn("date_key_str", F.trim(F.col("date_key")))

date_key = parse_date_multi(F.col("date_key_str"),
    ["M/d/yy","dd-MM-yyyy","M/d/yyyy","MM-dd-yyyy"])

dt_parsed = (dt
    .withColumn("date_key", date_key)
    .withColumn("is_weekend", norm_bool("is_weekend"))
    .withColumn("is_holiday", norm_bool("is_holiday"))
)

dt_bad = dt_parsed.filter(F.col("date_key").isNull())
if dt_bad.take(1):
    (dt_bad.write.mode("overwrite")
        .csv(f"s3://{S3_BUCKET}/{REJECT_PREFIX}/date_dim/{RUN_TS}/"))

dt_good = dt_parsed.filter(F.col("date_key").isNotNull())

dt_good = (dt_good
    .withColumn("year", F.when(F.col("year").rlike(r'^\d+$'), F.col("year").cast("int"))
                        .otherwise(F.year("date_key")))
    .withColumn("week", F.when(F.col("week").rlike(r'^\d+$'), F.col("week").cast("int"))
                        .otherwise(F.weekofyear("date_key")))
    .withColumn("month_name_from_date", F.date_format("date_key","MMMM"))
    .withColumn("month",
        F.when(F.col("month").rlike(r'^\d+$'),
               F.date_format(F.to_date(F.concat_ws('-',F.col("year"),F.col("month").cast("int"),F.lit(1))),"MMMM"))
         .otherwise(F.coalesce(F.col("month"), F.col("month_name_from_date"))))
    .withColumn("day_of_week", F.date_format("date_key","EEEE"))
    .select("date_key","day_of_week","week","month","year","is_weekend","is_holiday","holiday_name")
)

(dt_good.write.format("jdbc")
  .options(**jdbc_opts)
  .option("dbtable","dbo.date_dim")
  .mode("append")
  .save())

print("Seed complete.")

