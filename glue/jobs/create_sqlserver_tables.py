import boto3, json
from pyspark.sql import SparkSession

SECRET_ID = "arn:aws:secretsmanager:us-east-1:881128007058:secret:rds!db-81411d85-6340-451d-beab-9c90cbbdcb8f-9AEiWu"
RDS_ENDPOINT = "bus-insights-sqlserver.cgxeok660j5l.us-east-1.rds.amazonaws.com"

creds = json.loads(boto3.client('secretsmanager').get_secret_value(SecretId=SECRET_ID)['SecretString'])
USER, PWD = creds['username'], creds['password']

spark = SparkSession.builder.appName("bootstrap-sqlserver").getOrCreate()
jvm = spark._jvm
DriverManager = jvm.java.sql.DriverManager

def exec_sql(dbname, sql):
    url = f"jdbc:sqlserver://{RDS_ENDPOINT}:1433;databaseName={dbname};encrypt=true;trustServerCertificate=true;loginTimeout=15"
    conn = DriverManager.getConnection(url, USER, PWD)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()

# 1) Create a clean DB name (no hyphens)
exec_sql("master", "IF DB_ID('appdb') IS NULL CREATE DATABASE appdb;")

# 2) Create tables if missing
# order_items table (fact table)
exec_sql("appdb", """
    IF OBJECT_ID('dbo.order_items', 'U') IS NULL
        CREATE TABLE dbo.order_items (
          app_name             varchar(64)      NOT NULL,  -- String
          restaurant_id        varchar(48)      NOT NULL,  -- String/Int in CSV -> store as string
          creation_time_utc    datetime2(3)     NOT NULL,  -- Timestamp (UTC)
          order_id             varchar(48)      NOT NULL,  -- String/Int -> store as string
          user_id              varchar(48)      NULL,      -- String/Int -> store as string
          printed_card_number  varchar(64)      NULL,      -- tokenized/masked (NO raw data)
          is_loyalty           bit              NOT NULL,  -- Boolean
          currency             char(3)          NOT NULL,  -- e.g., USD
          lineitem_id          varchar(64)      NOT NULL,  -- String/Int -> store as string
          item_category        varchar(64)      NULL,      -- Item category (e.g. Beverage, Entree)
          item_name            varchar(128)     NULL,      -- Name of item
          item_price           decimal(10,2)    NOT NULL,  -- Decimal unit price of item
          item_quantity        int              NOT NULL,  -- Integer quantity of item purchased
          CONSTRAINT PK_order_items PRIMARY KEY (order_id, lineitem_id)
        );
    """)

# order_items_options table
exec_sql("appdb", """
    IF OBJECT_ID('dbo.order_items_options', 'U') IS NULL
        CREATE TABLE dbo.order_items_options (
            order_id            varchar(48)      NOT NULL,  -- String/Int; Identifier linking to parent order
            lineitem_id         varchar(64)      NOT NULL,  -- String/Int; Identifier linking to the specific order item
            option_group_name   varchar(64)      NULL,      -- Group category for options (e.g. Size, Toppings)
            option_name         varchar(128)     NOT NULL,  -- Selected option or customization (e.g. Extra Cheese)
            option_price        decimal(10,2)    NOT NULL,  -- Price of option (negative if it's a discount)
            option_quantity     int              NOT NULL,  -- Number of times the option was added
            CONSTRAINT PK_order_item_options PRIMARY KEY (order_id, lineitem_id, option_name),
            CONSTRAINT FK_options_items FOREIGN KEY (order_id, lineitem_id)
            REFERENCES dbo.order_items(order_id, lineitem_id)        
        );
    """)

# date_dim table
exec_sql("appdb", """
    IF OBJECT_ID('dbo.date_dim', 'U') IS NULL
        CREATE TABLE dbo.date_dim (
          date_key     date          NOT NULL,   -- Full calendar date
          day_of_week  varchar(16)   NOT NULL,   -- Day of week (e.g., Monday)
          week         smallint      NOT NULL,   -- Week number in year, Integer
          month        varchar(16)   NOT NULL,   -- Month name (e.g., January)
          [year]       int           NOT NULL,   -- Calendar year; use sq brackets to indicate field, not reserved word
          is_weekend   bit           NOT NULL,   -- Whether date falls on weekend, boolean
          is_holiday   bit           NOT NULL,   -- Whether date is a holiday, boolean
          holiday_name varchar(64)   NULL,       -- Name of holiday (if applicable)
          CONSTRAINT PK_date_dim PRIMARY KEY (date_key)
        );
""")

print("Bootstrap complete: appdb + tables created.")























