# Databricks notebook source
# DBTITLE 1,Notebook intro
# MAGIC %md
# MAGIC ## Exploration: `fact_order_items`
# MAGIC
# MAGIC This notebook explores the transformation logic for the `fact_order_items` silver table.
# MAGIC
# MAGIC The source is `ws_restaurant_dbxproject.01_bronze.orders`, which contains an `items` array column. Each element in the array represents a single ordered item. The goal is to explode that array into one row per item, cast columns to the correct types, and compute a `subtotal` per line item.

# COMMAND ----------

df=spark.read.table('ws_restaurant_dbxproject.`01_bronze`.orders')
display(df.limit(1))

# COMMAND ----------

# DBTITLE 1,Transformation explanation
# MAGIC %md
# MAGIC ## Transformation — Explode items array
# MAGIC
# MAGIC Using `LATERAL VIEW EXPLODE(items) AS item` to flatten the nested `items` array from the bronze `orders` table into individual rows. Each resulting row represents one line item of an order.
# MAGIC
# MAGIC Columns are explicitly cast to target types, and `subtotal` is derived as `unit_price * quantity`.

# COMMAND ----------

# DBTITLE 1,Cell 2
'''
order_id STRING,
order_timestamp TIMESTAMP,
order_date DATE,
restaruant_id STRING,
------------------------
item_id STRING,
item_name STRING,
category STRING,
quantity INT,
unit_price DECIMAL(10,2),
subtotal DECIMAL(10,2)
'''
fact_menu_items=(
    spark.sql('''
      select 
      CAST(order_id AS STRING) AS order_id,
      CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
      CAST(to_date(order_timestamp) AS DATE) AS order_date,
      CAST(restaurant_id AS STRING) AS restaurant_id,

      CAST(item.item_id AS STRING) AS item_id,
      CAST(item.name AS STRING) AS item_name,
      CAST(item.category AS STRING) AS category,
      CAST(item.quantity AS INT) AS quantity,
      CAST(item.unit_price AS DECIMAL(10,2)) AS unit_price,
      CAST(item.unit_price * item.quantity AS DECIMAL(10,2)) AS subtotal

      from ws_restaurant_dbxproject.`01_bronze`.orders
      lateral view explode(items) as item'''
    )
)

# COMMAND ----------

# DBTITLE 1,Verification explanation
# MAGIC %md
# MAGIC ## Verification
# MAGIC
# MAGIC Filter by a specific `order_id` to confirm that the array was correctly exploded — each row should correspond to one item in the original order, with correct types and a computed `subtotal`.

# COMMAND ----------

display(fact_menu_items.filter(fact_menu_items['order_id'] == 'ORD-20260627-428220'))
