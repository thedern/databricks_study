# Databricks notebook source
# MAGIC %md
# MAGIC ### ORDER_DETAILS tables
# MAGIC Using Silver data parquet files, create an order_details table with the following attributes:
# MAGIC - ORDER ID
# MAGIC - ORDER DATE
# MAGIC - CUSTOMER ID
# MAGIC - STORE NAME
# MAGIC - TOTAL ORDER AMOUNT
# MAGIC
# MAGIC The table should be aggregated by ORDER ID, ORDER DATE, CUSTOMER ID, and STORE NAME to show the TOTAL ORDER AMOUNT
# MAGIC
# MAGIC Order of operations is key  

# COMMAND ----------

"""
Import data
"""

order_items = spark.read.parquet('/FileStore/sample_data_u/silver/order_items')
products = spark.read.parquet('/FileStore/sample_data_u/silver/products')
customers = spark.read.parquet('/FileStore/sample_data_u/silver/customers')
orders = spark.read.parquet('/FileStore/sample_data_u/silver/orders')


# COMMAND ----------

""" 
Verify orders by displaying DF
"""
orders.display()

# COMMAND ----------

"""
Verify orders data types
"""
orders.dtypes

# COMMAND ----------

"""
We will change the order_timestamp to a data using the 'to_date' function
We will capture the change via a select statement, saving to an 'order_details' df, perserving 'orders' in-case we need it later
"""
from pyspark.sql.functions import to_date
order_details = orders.select('ORDER_ID', to_date("ORDER_TIMESTAMP").alias('DATE'), "CUSTOMER_ID", "STORE_NAME")
order_details.display()

# COMMAND ----------

order_details.dtypes

# COMMAND ----------

"""
Verify order_items
"""
order_items.display()

# COMMAND ----------

order_items.dtypes

# COMMAND ----------

"""
Join order_details df and order_items df on the 'order_id' column of both tables
- left join as we want all rows from order_details, not just where each df match
select the relevant columns, storing back into order_details
"""
from pyspark.sql.functions import col

order_details = order_details.join(order_items, order_items['ORDER_ID']==order_details['ORDER_ID'], 'left'). \
    select(order_details['ORDER_ID'], order_details['DATE'], order_details['CUSTOMER_ID'], order_details['STORE_NAME'], order_items['UNIT_PRICE'], order_items['QUANTITY'])
order_details.display()

# COMMAND ----------

"""
Add new column 'TOTAL_SALES_AMOUNT' by multiplying UNIT_PRICE * QUANITY
round TOTAL_SALES_AMOUNT to two decimal places
Retain by saving back into order_details df
"""
from pyspark.sql.functions import round
order_details = order_details.withColumn('TOTAL_SALES_AMOUNT', round(order_details['UNIT_PRICE'] * order_details['QUANTITY'], 2))
order_details.display()

# COMMAND ----------

"""
Group the order_details df and taking the sum of the total sales amount, renaming to 'TOTAL_ORDER_AMOUNT'
This will consolidate ORDER_IDs to a single record
Save by assigning back to dataframe
"""
from pyspark.sql.functions import asc
order_details = order_details.groupBy('ORDER_ID', 'DATE', 'CUSTOMER_ID', 'STORE_NAME'). \
    sum('TOTAL_SALES_AMOUNT'). \
        withColumnRenamed('sum(TOTAL_SALES_AMOUNT)', 'TOTAL_ORDER_AMOUNT')


# COMMAND ----------

"""
Display Results
"""
from pyspark.sql.functions import asc
order_details.sort(order_details['ORDER_ID'].asc()).display()
# order_details.display()

# COMMAND ----------

"""
Round TOTAL_ORDER_AMOUYNT to two decimal places by replacing it with a new version of the same column
"""
order_details = order_details.withColumn('TOTAL_ORDER_AMOUNT', round('TOTAL_ORDER_AMOUNT', 2))
order_details.sort(order_details['ORDER_ID'].asc()).display()

# COMMAND ----------

"""
Save work to Gold
"""
order_details.write.parquet('/FileStore/sample_data_u/gold/order_details', mode='overwrite')

# COMMAND ----------

# verify
%fs ls '/FileStore/sample_data_u/gold'

# COMMAND ----------

# MAGIC %md
# MAGIC ### MONTHLY_SALES TABLE
# MAGIC
# MAGIC Create an aggregated table to show the monthly sales total and save it in the gold layer as a parquet file:  MONTHLY_SALES
# MAGIC
# MAGIC Table should have two cols
# MAGIC - MONTH_YEAR - format:   yyyy-MM example;  2020-10
# MAGIC - TOTAL_SALES
# MAGIC
# MAGIC Display rounded to 2 decimal places, sorted desc
# MAGIC
# MAGIC We cam leverage the order_details df for this work
# MAGIC

# COMMAND ----------

"""
Create a column that extracts the month and the year from the date column
"""
from pyspark.sql.functions import date_format
sales_with_month = order_details.withColumn('MONTH_YEAR', date_format('DATE', 'yyyy-MM'))
sales_with_month.display()

# COMMAND ----------

"""
Group by MONTH_YEAR and aggregate by TOTAL_ORDER_AMOUNT
Round to two decimal places, renaming column to TOTAL_SALES
Save off only MONTH_YEAR, and TOTAL_SALES sorted desc
"""
monthly_sales = sales_with_month.groupBy('MONTH_YEAR').sum('TOTAL_ORDER_AMOUNT'). \
    withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)).sort(sales_with_month['MONTH_YEAR'].desc()). \
        select('MONTH_YEAR', 'TOTAL_SALES')

# COMMAND ----------

monthly_sales.display()

# COMMAND ----------

"""
Save to Gold
"""
monthly_sales.write.parquet('FileStore/sample_data_u/gold/monthly_sales', mode='overwrite')

# COMMAND ----------

# MAGIC
# MAGIC %fs ls 'FileStore/sample_data_u/gold'

# COMMAND ----------

# MAGIC %md
# MAGIC ### STORE_MONTHLY_SALES TABLE
# MAGIC
# MAGIC Create an aggregated table to show the monthly sales total by store and save it in the gold layer as a parquet file: STORE_MONTHLY_SALES
# MAGIC
# MAGIC Table should have three cols
# MAGIC
# MAGIC MONTH_YEAR - format: yyyy-MM example; 2020-10
# MAGIC TOTAL_SALES
# MAGIC STORE_NAME
# MAGIC
# MAGIC Display rounded to 2 decimal places, sorted desc
# MAGIC
# MAGIC We cam leverage the sales_with_month df for this work

# COMMAND ----------

store_monthly_sales = sales_with_month.groupBy('MONTH_YEAR', 'STORE_NAME').sum('TOTAL_ORDER_AMOUNT'). \
    withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)).sort(sales_with_month['MONTH_YEAR'].desc()). \
        select('MONTH_YEAR', 'STORE_NAME', 'TOTAL_SALES')

# COMMAND ----------

store_monthly_sales.display()

# COMMAND ----------

"""
Save to Gold
"""
store_monthly_sales.write.parquet('FileStore/sample_data_u/gold/store_monthly_sales', mode='overwrite')

# COMMAND ----------

# MAGIC %fs ls '/FileStore/sample_data_u/gold'
