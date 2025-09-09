import sys, datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F, Window

args = getResolvedOptions(sys.argv, ['BUCKET'])
BUCKET = args['BUCKET'].strip().strip('"').strip("'")
PROCESS_DATE = args.get('PROCESS_DATE') or (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

spark = (SparkSession.builder.appName("job-name").getOrCreate())
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def read(path):  return spark.read.parquet(f"s3://{BUCKET}/{path}")
def write(df, path, part): (df.write.mode("overwrite")
                               .option("compression","snappy")
                               .partitionBy(part)
                               .parquet(f"s3://{BUCKET}/{path}"))

# TODO: your logic here (read → transform → groupBy → write)

