import json, boto3
from pyspark.sql import SparkSession

SECRET_ID = "REDACTED"
JDBC_URL  = "REDACTED"

creds = json.loads(boto3.client('secretsmanager').get_secret_value(SecretId=SECRET_ID)['SecretString'])
spark = SparkSession.builder.appName("jdbc-smoke-test").getOrCreate()

df = (spark.read.format("jdbc")
      .option("url", JDBC_URL)
      .option("user", creds['username'])
      .option("password", creds['password'])
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", "(SELECT 1 AS ok) t")
      .load())

print("Connected OK. Rowcount =", df.count())
