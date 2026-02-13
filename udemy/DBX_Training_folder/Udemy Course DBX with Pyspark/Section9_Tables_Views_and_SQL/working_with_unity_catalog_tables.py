# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating Managed Tables
# MAGIC Managed tables are the default table type in DBX

# COMMAND ----------

file_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/csv_data/countries_population/countries_population.csv'

# COMMAND ----------

"""
create schema for data types
create dataframe from csv file
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_1 = StructType([
    StructField("country_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("iso_alpha2", StringType(), True),
    StructField("capital", StringType(), True),
    StructField("population", IntegerType(), True),
    StructField("area_km2", IntegerType(), True),
    StructField("region_id", IntegerType(), True),
    StructField("sub_region_id", IntegerType(), True),

])

countries_df = spark.read.csv(file_path, header=True, schema=schema_1)
countries_df.display()

# COMMAND ----------

"""
Save data frame as table, need to use the catalog.schema.table hierarchy
saveAsTable
Default is managed table type in with a data source in delta format
NOTE: managed tables are always delta, you cannot change the format
"""
countries_df.write.saveAsTable('workspace.pyspark_learning.countries_table')

# COMMAND ----------

"""
to return the table as a data frame, use the read.table method
"""
spark.read.table('workspace.pyspark_learning.countries_table').display()


# COMMAND ----------

"""
Can write that data frame back to the same table if use append or overwrite modes
"""
country_table_df = spark.read.table('powerplatform_administration_development_2.powerplatform_administration.countries_table')
# schema is written into the dataframe because the data has types assigned in the table
country_table_df.dtypes
country_table_df.write.saveAsTable('powerplatform_administration_development_2.powerplatform_administration.countries_table', mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Queries with pyspark
# MAGIC
# MAGIC Why would we do this and not just run the equivalent pyspark, `spark.read.table()`?
# MAGIC Because, there are certain untity catalog operations for which there is no spark command and SQL must be used.

# COMMAND ----------

"""
SQL synatx as an argument to the pyspark API
A dataframe is returned

"""
spark.sql("SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table").display()

# This is equivalent to:
# spark.read.table('powerplatform_administration_development_2.powerplatform_administration.countries_table').display()



# COMMAND ----------

# MAGIC %sql
# MAGIC -- the cell above is the same as running this in a SQL cell
# MAGIC SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Managed Table using SQL Syntax
# MAGIC This is functionally equivalent to the spark syntax for `saveAsTable()`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creates empty table with associated schema
# MAGIC CREATE TABLE powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql
# MAGIC (
# MAGIC   country_id INTEGER,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   country_code STRING,
# MAGIC   iso_alpha2 STRING,
# MAGIC   capital STRING,
# MAGIC   population INTEGER,
# MAGIC   area_km2 INTEGER,
# MAGIC   region_id INTEGER,
# MAGIC   sub_region_id INTEGER
# MAGIC )

# COMMAND ----------

""" 
The table is empty
Read table to show this as TRUE
"""
spark.sql('SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql').display()
# spark.read.table('powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql').display()

# COMMAND ----------

"""
Insert data
To do this, I will read from our first countries table and insert into the new table
Then, I will display the contents of the new table
I will put both spark.sql and spark.read.table examples below
"""
# 1. get data from countries_table and save to data frame

country_table_df_1 = spark.sql("SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table")
# country_table_df_1 = spark.read.table('powerplatform_administration_development_2.powerplatform_administration.countries_table')

# 2. write table from data frame to new table, countries_table_from_sql
country_table_df_1.write.saveAsTable('powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql', mode='append')

# 3. read contents of new table, countries_table_from_sql
spark.sql("SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql").display()
# spark.read.table('powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql').display()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table from results of select from another table
# MAGIC CREATE TABLE powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql_2
# MAGIC AS
# MAGIC SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql

# COMMAND ----------

"""
Show contents of contries_table_from_sql_2
"""
spark.sql("SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql_2").display()
# spark.read.table('powerplatform_administration_development_2.powerplatform_administration.countries_table_from_sql_2').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Views with SQL
# MAGIC A view is a read only object as a result of a select over one or more existing tables or other views.
# MAGIC These are persistent objects

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW powerplatform_administration_development_2.powerplatform_administration.countries_top_10 AS
# MAGIC SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_table ORDER BY population DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_top_10

# COMMAND ----------

"""
Equivalent pyspark to the SQL above
"""
# spark.sql("SELECT * FROM powerplatform_administration_development_2.powerplatform_administration.countries_top_10").display()
spark.read.table("powerplatform_administration_development_2.powerplatform_administration.countries_top_10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema
# MAGIC All code will be commented, I don't actually want to do this here

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CREATE CATALOG test_catalog
# MAGIC -- MANAGED LOCATION "abfss://account-data@use2sto004mdpdev07.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a schema, syntax is catalog.schema name
# MAGIC CREATE SCHEMA powerplatform_administration_development_2.pyspark_test_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a volume under new schema, syntax is catalog.schema.volume
# MAGIC -- I noticed the volume did not show up in the explorer on the left nave but was accessible in the right-hand pane.  As soon as I created a raw files sub directory, the volume and the subdirectory showed up in the left-explorer
# MAGIC CREATE VOLUME powerplatform_administration_development_2.pyspark_test_schema.pyspark_test_volume

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop/Delete Unity Catalog Items

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop catalog
# MAGIC -- must used CASCADE if not empty
# MAGIC -- DROP CATALOG <NAME>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop schema
# MAGIC -- must used CASCADE if not empty
# MAGIC -- full namespace
# MAGIC -- DROP SCHEMA <name> CASCADE
# MAGIC DROP SCHEMA powerplatform_administration_development_2.pyspark_test_schema CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA workspace.pyspark CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop volume
# MAGIC -- no cascade
# MAGIC -- full namespace
# MAGIC -- DROP VOLUME <name> CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table
# MAGIC -- no cascade
# MAGIC -- full namespace
# MAGIC -- DROP TABLE <name> 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop view
# MAGIC -- no cascade
# MAGIC -- full namespace
# MAGIC -- DROP VIEW <name> 
