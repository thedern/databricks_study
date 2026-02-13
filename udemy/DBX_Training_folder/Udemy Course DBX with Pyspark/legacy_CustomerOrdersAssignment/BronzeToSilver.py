# Databricks notebook source
# MAGIC %fs ls '/FileStore/sample_data_u/bronze'

# COMMAND ----------

# import modules
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, StructField

# COMMAND ----------

# import functions
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

"""
Read in ORDER_ITEMS csv to pyspark_df
1. Create Schema - visually inspect the csv and identify columns:  ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY
2. Read in Data
3. Display as a check
"""
order_item_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("LINE_ITEM_ID", IntegerType(), False),
        StructField("PRODUCT_ID", IntegerType(), False),
        StructField("UNIT_PRICE", DoubleType(), False),
        StructField("QUANTITY", IntegerType(), False)
    ]
)
order_items_df = spark.read.csv('/FileStore/sample_data_u/bronze/order_items.csv', header=True, schema=order_item_schema)
order_items_df.display()

# COMMAND ----------

"""
Drop LINE_ITEM_ID from df
"""
order_items_df = order_items_df.drop('LINE_ITEM_ID')
order_items_df.display()

# COMMAND ----------

"""
Read in CUSTOMERS csv to pyspark_df
1. Create Schema - visually inspect the csv and identify columns:  CUSTTOMER_ID, FULL_NAME, EMAIL_ADDRESS
2. Read in Data
3. Display as a check
"""
customer_schema = StructType(
    [
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("FULL_NAME", StringType(), False),
        StructField("EMAIL_ADDRESS", StringType(), False)
    ]
)
customer_df = spark.read.csv('/FileStore/sample_data_u/bronze/customers.csv', header=True, schema=customer_schema)
customer_df.display()

# COMMAND ----------

"""
Read in PRODUCTS csv to pyspark_df
1. Create Schema - visually inspect the csv and identify columns:  PRODUCT_ID, PRODUCT_NAME, UNIT_PRICE
2. Read in Data
3. Display as a check
"""
product_schema = StructType(
    [
        StructField("PRODUCT_ID", IntegerType(), False),
        StructField("PRODUCT_NAME", StringType(), False),
        StructField("UNIT_PRICE", DoubleType(), False)
    ]
)
product_df = spark.read.csv('/FileStore/sample_data_u/bronze/products.csv', header=True, schema=product_schema)
product_df.display()

# COMMAND ----------

"""
Read in STORES csv to pyspark_df
1. Create Schema - visually inspect the csv and identify columns:  STORE_ID, STORE_NAME, WEB_ADDRESS, LATITUDE, LONGITUDE
2. Read in Data
3. Display as a check
"""
stores_schema = StructType(
    [
        StructField("STORE_ID", IntegerType(), False),
        StructField("STORE_NAME", StringType(), False),
        StructField("WEB_ADDRESS", StringType(), False),
        StructField("LATITUDE", DoubleType(), False),
        StructField("LONGITUDE", DoubleType(), False)
    ]
)
stores_df = spark.read.csv('/FileStore/sample_data_u/bronze/stores.csv', header=True, schema=stores_schema)
stores_df.display()

# COMMAND ----------

"""
Read in ORDERS csv to pyspark_df
1. Create Schema - visually inspect the csv and identify columns:  ORDER_ID, ORDER_DATETIME, CUSTOMER_ID, ORDER_STATUS, STORE_ID
2. Read in Data
3. Display as a check
"""

orders_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("ORDER_DATETIME", StringType(), False),
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("ORDER_STATUS", StringType(), False),
        StructField("STORE_ID", IntegerType(), False)
    ]
)
orders_df = spark.read.csv('/FileStore/sample_data_u/bronze/orders.csv', header=True, schema=orders_schema)
orders_df.display()


# COMMAND ----------

"""
Filter Out all incomplete order from orders_df
"""
orders_df = orders_df.filter(orders_df['ORDER_STATUS'] == 'COMPLETE')
orders_df.display()

# COMMAND ----------

"""
Convert and rename ORDER_DATETIME to ORDER_TIMESTAMP
Overwrite current DF
"""

orders_df = orders_df.select('ORDER_ID', \
    to_timestamp(orders_df['order_datetime'], "dd-MMM-yy kk.mm.ss.SS").alias('ORDER_TIMESTAMP'), \
        'CUSTOMER_ID', \
        'ORDER_STATUS', \
        'STORE_ID'
    )
orders_df.display()

# COMMAND ----------

"""
JOIN orders and stores DF
Reduce to 'ORDER_ID', 'ORDER_TIMESTAMP', 'CUSTOMER_ID', 'STORE_NAME'
"""

orders_final_df = orders_df.join(stores_df, orders_df['STORE_ID'] == stores_df['STORE_ID'], 'inner'). \
    select('ORDER_ID', 'ORDER_TIMESTAMP', 'CUSTOMER_ID', 'STORE_NAME')
orders_final_df.display()

# COMMAND ----------

"""
Write to SILVER as parquet

"""
order_items_df.write.parquet('/FileStore/sample_data_u/silver/order_items', mode='overwrite')
product_df.write.parquet('/FileStore/sample_data_u/silver/products', mode='overwrite')
customer_df.write.parquet('/FileStore/sample_data_u/silver/customers', mode='overwrite')
orders_final_df.write.parquet('/FileStore/sample_data_u/silver/orders', mode='overwrite')


# COMMAND ----------

# MAGIC %fs ls '/FileStore/sample_data_u/silver'

# COMMAND ----------

# MAGIC %fs ls '/FileStore/sample_data_u/silver/orders'
