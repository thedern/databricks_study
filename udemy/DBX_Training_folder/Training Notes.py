# Databricks notebook source
# MAGIC %md
# MAGIC ## Deleveloping code in databricks notebooks
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebooks-code
# MAGIC ### Python Files - Modularize
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/notebooks/share-code
# MAGIC ### Imports
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/files/workspace-modules
# MAGIC ### Pyspark API
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

print('hello')

# COMMAND ----------

# MAGIC %md
# MAGIC Markdown
# MAGIC # This is a header
# MAGIC ### this is a subheader
# MAGIC this is text

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils provided file system utilites

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils file system method

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC list directory contents with:  'dbutils.fs.ls()'
# MAGIC

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables/')

# COMMAND ----------

# MAGIC %md
# MAGIC there is a magic command for the filestore, '%fs'.   This is similar to using python's dbutils.fs   The output of the magic command is presented as a table and may be eaiser to use while exploring data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/FileStore/tables/'

# COMMAND ----------


