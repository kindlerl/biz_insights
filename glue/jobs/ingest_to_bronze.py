import sys
from datetime import date
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

args = getResolvedOptions(sys.argv, ['DB_USER','DB_PASS','RDS_HOST','DB_NAME','BRONZE_S3'])
def clean(s): return s.strip().strip('"').strip("'")
DB_USER = clean(args['DB_USER']); DB_PASS = args['DB_PASS']
RDS_HOST= clean(args['RDS_HOST']); DB_NAME = clean(args['DB_NAME'])
BRONZE  = clean(args['BRONZE_S3'])

spark = SparkSession.builder.appName("bronze-extract").getOrCreate()
jdbc_url = f"jdbc:sqlserver://{RDS_HOST}:1433;databaseName={DB_NAME};encrypt=true;trustServerCertificate=true"
jdbc_base = {
    "url": jdbc_url, "user": DB_USER, "password": DB_PASS,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000"
}

today = date.today().isoformat()
tables = [
    ("dbo.order_items",         "order_items"),
    ("dbo.order_items_options", "order_items_options"),
    ("dbo.date_dim",            "date_dim"),
]

def log_count(label, df):
    c = df.count(); print(f"{label}: {c}"); return c

for dbtable, short in tables:
    print(f"Extracting {dbtable} ...")
    df = (spark.read.format("jdbc").options(**jdbc_base)
            .option("dbtable", dbtable).load())

    out = (df.withColumn("ingestion_date", F.lit(today)))

    log_count(f"{short}_rows_out", out)

    (out.write
        .mode("overwrite")                    # FIRST RUN ONLY → change to "append" later
        .partitionBy("ingestion_date")
        .option("compression","snappy")
        .parquet(f"s3://{BRONZE}/bronze/{short}/"))

print("Bronze extract complete.")

