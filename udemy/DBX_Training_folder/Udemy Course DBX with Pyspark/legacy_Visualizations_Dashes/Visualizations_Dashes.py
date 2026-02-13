# Databricks notebook source
"""
Read in data from GOLD layer (See section 6)
"""

order_details = spark.read.parquet('/FileStore/sample_data_u/gold/order_details')
monthly_sales = spark.read.parquet('/FileStore/sample_data_u/gold/monthly_sales')

# COMMAND ----------

"""
Display Dataframe, click on '+' icon to profile or visualize data
NOTE: to profile or visualize, must use 'display(<dataframe>)' syntax
NOTE: add more visualizations etc, by clicking '+' again...
"""
display(order_details)

# COMMAND ----------

"Monthly Sales Visualization"
display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Title:  Some makrdown to add to my dashboard

# COMMAND ----------

displayHTML("""<font size="6" color="blue" face="sans-serif">Sales Dashboard</font>""")
