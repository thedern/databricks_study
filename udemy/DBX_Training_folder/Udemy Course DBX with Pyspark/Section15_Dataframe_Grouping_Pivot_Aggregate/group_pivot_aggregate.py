# Databricks notebook source
# MAGIC %md
# MAGIC ### Group and Aggregate
# MAGIC
# MAGIC the dataframe 'groupBy' method allows for the grouping of data for further aggregation

# COMMAND ----------

"""
Get data for this notebook
"""
consolidated_df = spark.read.table('powerplatform_administration_development_2.pyspark_learning.countries_consolidated')

# COMMAND ----------

"""
the dataframe 'groupBy' method allows for the grouping of data for further aggregation
Takes a list of column objects
Below I will group the dataframe by region column
Creates a grouped data object
"""
type(consolidated_df.groupBy("region"))

# COMMAND ----------

"""
to use a group object, one must do something with it
Below I am doing a 'sum' aggregation on population by region
NOTE: works by refernce.  I tried dot notation below and got an error.
"""
from pyspark.sql.functions import sum
consolidated_df.groupBy("region").sum("population").display()

# COMMAND ----------

"""
To do more than one aggregation at a time, you must use the 'agg' method and place all your aggregations in it
Below I want the sum and the average
"""
from pyspark.sql.functions import sum, avg

consolidated_df.groupBy('region').agg(
    sum('population'),
    avg('population')
).display()

# COMMAND ----------

"""
Remember to alias to clean up column mames
By default, columns will be named after the operation which created them
"""
consolidated_df.groupBy('region').agg(
    sum('population').alias('Total Population'),
    avg('population').alias('Average Population')
).display()

# COMMAND ----------

"""
Can group by multiple columns and order the results
"""

consolidated_df.groupBy('region', 'sub_region').agg(
    sum('population').alias('Total Population'),
    avg('population').alias('Average Population')
).orderBy('region').display()

# COMMAND ----------

"""
Can group by multiple columns and order the results
'sort' and 'orderBy' have the same effect.
NOTE: I could not get column objects to work here without error so everthing is by reference
"""

consolidated_df.groupBy('region', 'sub_region').agg(
    sum('population').alias('Total_Population'),
    avg('population').alias('Average_Population')
).sort('Total_Population', ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivot
# MAGIC
# MAGIC Turns unique values in a column to multiple new columns

# COMMAND ----------

"""
You can pivot on a grouped object
Here we grouped on sub_region and turned each region into column
"""

consolidated_df.groupBy("sub_region").pivot("region").sum("population").display()

# COMMAND ----------

"""
this is the inverse of above which for some reason makes more logical sense to me
"""

consolidated_df.groupBy("region").pivot("sub_region").sum("population").display()

# COMMAND ----------



# COMMAND ----------

"""
You can limit your piviot by selecting specific columns
Input those columns as a list

"""
consolidated_df.groupBy("sub_region").pivot("region", ['America', 'Asia']).sum("population").display()

# COMMAND ----------

"""
unpivot
"""

