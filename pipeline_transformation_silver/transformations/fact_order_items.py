from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import *

@dp.table(name="02_silver.fact_order_items", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(
    {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_order_timestamp": "order_timestamp IS NOT NULL",
        "valid_item_id": "item_id IS NOT NULL",
        "valid_restaurant_id": "restaurant_id IS NOT NULL",
        "valid_quantity": "quantity > 0",
        "valid_unit_price": "unit_price > 0",
        "valid_subtotal": "subtotal > 0",
    }
)
def fact_order_items():
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
    return fact_menu_items