# Databricks notebook source
"""
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html
This is the entry point to programming Spark, with the Dataset and DataFrame API
This is a unified object that provides an entry point into programming with Spark

THIS IS NOT ACTUALLY REQUIRED IN DBX, ... its already provided for you, ready to use by DBX
Its available automatically once a DBX notebook is created.
This code is handy for working with pyspark OUTSIDE of DBX
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

"""
Anatomy of spark syntax
Example 1, step-by-step syntax
Each line could also exist in its own cell as well
This approach is good for testing as lines are isolated
"""

df = spark.read.format('csv').load('/path_to/file')
df = df.filter("Age > 40")
df.write.format('csv').save('path_to/file')


# COMMAND ----------

"""
Anatomy of spark syntax
Example 1, chaining syntax, all on one line
The same df is created here as well, however its implicit and not explict as it is in the cell above
"""
spark.read.format('csv').load('path_to/file').filter("Age < 50").write.format('csv').save('path_to/file')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
