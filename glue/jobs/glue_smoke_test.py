import json, boto3
from pyspark.sql import SparkSession

SECRET_ID = "arn:aws:secretsmanager:us-east-1:881128007058:secret:rds!db-81411d85-6340-451d-beab-9c90cbbdcb8f-9AEiWu"
JDBC_URL  = "jdbc:sqlserver://bus-insights-sqlserver.cgxeok660j5l.us-east-1.rds.amazonaws.com:1433;databaseName=master;encrypt=true;trustServerCertificate=false;loginTimeout=15"

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
