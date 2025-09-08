import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# params (make sure there are NO quotes/spaces in the Job parameters)
args = getResolvedOptions(sys.argv, ['DB_USER','DB_PASS','RDS_HOST','DB_NAME'])

def clean(s): return s.strip().strip('"').strip("'")

DB_USER = clean(args['DB_USER'])
DB_PASS = args['DB_PASS']          # don't strip passwords
RDS_HOST= clean(args['RDS_HOST'])
DB_NAME = clean(args['DB_NAME'])

spark = SparkSession.builder.appName("row-counts").getOrCreate()

JDBC_URL = f"jdbc:sqlserver://{RDS_HOST}:1433;databaseName={DB_NAME};encrypt=true;trustServerCertificate=true"

def scalar(sql: str) -> int:
    df = (spark.read.format("jdbc")
          .option("url", JDBC_URL)
          .option("user", DB_USER)
          .option("password", DB_PASS)
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
          .option("dbtable", f"({sql}) t")
          .load())
    return int(df.collect()[0][0])

# ⚠️ Use the exact table names you created. If your DDL used order_items_options (with 's'), use that here:
counts = {
    "order_items"         : scalar("SELECT COUNT(*) AS oi_count FROM dbo.order_items"),
    "order_items_options" : scalar("SELECT COUNT(*) AS oio_count FROM dbo.order_items_options"),
    "date_dim"            : scalar("SELECT COUNT(*) AS dd_count FROM dbo.date_dim"),
}

print("ROW COUNTS:", counts)

