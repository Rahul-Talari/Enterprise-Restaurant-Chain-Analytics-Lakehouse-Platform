# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer: fact_orders = Historical Database orders + Event Hubs Orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fact_orders`
# MAGIC
# MAGIC **Grain:** One record per customer order.
# MAGIC
# MAGIC | Column | Data Type | Description |
# MAGIC |--------|-----------|-------------|
# MAGIC | `order_id` | STRING | Unique identifier for each order. **Primary Key** |
# MAGIC | `order_timestamp` | TIMESTAMP | Timestamp when the order was placed. |
# MAGIC | `order_date` | DATE | Date on which the order was placed. |
# MAGIC | `order_hour` | INT | Hour of the day (0–23) when the order was placed. |
# MAGIC | `day_of_week` | STRING | Day of the week on which the order was placed. |
# MAGIC | `is_weekend` | BOOLEAN | Indicates whether the order was placed on a weekend. |
# MAGIC | `restaurant_id` | STRING | Identifier of the restaurant fulfilling the order. |
# MAGIC | `customer_id` | STRING | Identifier of the customer who placed the order. |
# MAGIC | `order_type` | STRING | Order type (Delivery, Takeaway, or Dine-in). |
# MAGIC | `item_count` | INT | Total number of menu items in the order. |
# MAGIC | `total_amount` | DECIMAL(10,2) | Total order value. |
# MAGIC | `payment_method` | STRING | Payment method used by the customer. |
# MAGIC | `order_status` | STRING | Current status of the order (e.g., Pending, Preparing, Delivered, Cancelled). |

# COMMAND ----------

'''
Lakeflow Declarative Pipeline Logic
    - bronze.historical_orders    : Initial historical load from the source database. Subsequent pipeline runs process only new or changed records.
    - bronze.orders               : Streaming ingestion from Event Hubs, continuously capturing new order events.
    - unionByName()               : Combines historical and streaming datasets while aligning columns by name.
    - dropDuplicates(["order_id"]): Removes duplicate orders, ensuring each order_id appears only once in fact_orders.
'''

from pyspark import pipelines as dp

@dp.table(
    name="fact_orders",
    table_properties={"quality": "silver"}
)
def fact_orders():

    historical_df = spark.readStream.table("ws_restaurant_dbxproject.`01_bronze`.historical_orders")
    orders_df = spark.readStream.table("ws_restaurant_dbxproject.`01_bronze`.orders")

    return (
        historical_df
        .unionByName(orders_df, allowMissingColumns=True)
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ws_restaurant_dbxproject.`01_bronze`.orders (
# MAGIC   order_id,
# MAGIC   order_timestamp,
# MAGIC   restaurant_id,
# MAGIC   customer_id,
# MAGIC   order_type,
# MAGIC   items,
# MAGIC   total_amount,
# MAGIC   payment_method,
# MAGIC   order_status
# MAGIC )
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_timestamp,
# MAGIC   restaurant_id,
# MAGIC   customer_id,
# MAGIC   order_type,
# MAGIC   from_json(items, 'ARRAY<STRUCT<item_id: STRING, name: STRING, category: STRING, quantity: INT, unit_price: DECIMAL(10,2), subtotal: DECIMAL(10,2)>>'),
# MAGIC   CAST(total_amount AS DOUBLE),
# MAGIC   payment_method,
# MAGIC   order_status
# MAGIC FROM ws_restaurant_dbxproject.`01_bronze`.historical_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Order Transformations

# COMMAND ----------

df=spark.read.table("ws_restaurant_dbxproject.`01_bronze`.orders")
df.printSchema()
display(df.limit(1))

# COMMAND ----------

from pyspark.sql.functions import *

'''
order_id STRING PRIMARY KEY,
order_timestamp TIMESTAMP,
order_date DATE,
order_hour INT,
day_of_week STRING,
is_weekend BOOLEAN,
restaurant_id STRING,
customer_id STRING,
order_type STRING,
item_count INT,
total_amount DECIMAL(10,2),
payment_method STRING,
order_status STRING
'''
SCHEMA='''ARRAY<STRUCT<item_id: STRING, name: STRING, category: STRING, quantity: INT, unit_price: DECIMAL(10,2), subtotal: DECIMAL(10,2)>>'''

df=spark.read.table("ws_restaurant_dbxproject.`01_bronze`.orders")
df=(df.withColumn("order_timestamp",to_timestamp(df.order_timestamp))
      .withColumn("order_date",to_date(df.order_timestamp))
      .withColumn("order_hour",hour(df.order_timestamp))
      .withColumn("day_of_week", date_format("order_timestamp", "EEEE"))
      .withColumn("is_weekend",
                   when(col('day_of_week').isin(["saturday","sunday"]),True)
                  .otherwise(False)
                  )
      .withColumn("items_parsed",from_json(to_json(col('items')),SCHEMA))
      .withColumn("item_count",size(df.items))
      .select("order_id","order_timestamp","order_date","order_hour","day_of_week","is_weekend","restaurant_id","customer_id","order_type","items_parsed","item_count","total_amount","payment_method","order_status")
)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP MATERIALIZED VIEW ws_restaurant_dbxproject.`02_silver`.fact_orders

# COMMAND ----------


