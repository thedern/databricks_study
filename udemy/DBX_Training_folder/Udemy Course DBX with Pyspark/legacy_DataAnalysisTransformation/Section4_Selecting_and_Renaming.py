# Databricks notebook source
# MAGIC %fs
# MAGIC ls '/FileStore/sample_data_u'

# COMMAND ----------

# Paths are absolute, not relative, need leading '/'
countries_path = '/FileStore/sample_data_u/countries.csv'
# imoort data manually setting schema and types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
countries_schema = StructType([
    StructField("COUNTRY_ID", IntegerType(), False),
    StructField("NAME", StringType(), False),
    StructField("NATIONALITY", StringType(), False),
    StructField("COUNTRY_CODE", StringType(), False),
    StructField("ISO_ALPHA2", StringType(), False),
    StructField("CAPITAL", StringType(), False),
    StructField("POPULATION", DoubleType(), False),
    StructField("AREA_KM2", IntegerType(), False),
    StructField("REGION_ID", IntegerType(), True),
    StructField("SUB_REGION_ID", IntegerType(), True),
    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
])

# COMMAND ----------

# read file into df
# uses the path to the file and schema set above
countries = spark.read.csv(path=countries_path, header=True, schema=countries_schema)

# COMMAND ----------

# display df preview
countries.display()

# COMMAND ----------

# select and display specific columns of each record
# this displays only, the syntax below does not support any methods on the columns requested
countries.select('name','capital','population').display()

# COMMAND ----------

# alternate select via keyname is more 'pythonic' AND it supports methods.  Prior shortcut select does not
countries.select(countries['name'],countries['capital'],countries['population']).display()

# COMMAND ----------

# renaming columns with 'alias' method.  Renames 'name' column to 'country'
# the index notation of 'countries['name'] supports the 'alias' method, which takes a string argument
# 'select' displays only renamed column
countries.select(countries['name'].alias('country')).display()

# COMMAND ----------

# dot notation can be used but is case-sensitive on column names
countries.select(countries.NAME,countries.CAPITAL,countries.POPULATION).display()

# COMMAND ----------

# col function can be used and supports methods as well
# select method only returns specific columns
from pyspark.sql.functions import col
countries.select(col('name'),col('capital'),col('population')).display()

# COMMAND ----------

# 'withColumnRenamed' method
# while more verbose than 'alias', withColumnRenamed is a more powerful/flexible method
countries.select('name', 'capital').withColumnRenamed('name','country_name').withColumnRenamed('capital', 'country_cap').display()

# COMMAND ----------

# 'withColumnRenamed' method
# removing 'select' displays whole dataframe with renamed columns
countries.withColumnRenamed('name','country_name').display()

# COMMAND ----------

'''
1. assign csv to variable
2. import data types
3. define a schema
4. load csv and apply schema to data
5. display data
'''

countries_regions = '/FileStore/sample_data_u/country_regions.csv'
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
countries_regions_schema = StructType([
    StructField("ID", IntegerType(), False),
    StructField("NAME", StringType(), False),
])
countries_regions_df = spark.read.csv(path=countries_regions, header=True, schema=countries_regions_schema)
countries_regions_df.display()

# COMMAND ----------

"""
1. select id and name columns only
2. display name as 'continent'
"""
countries_regions_df.select('id','name').withColumnRenamed('name','continent').display()

# COMMAND ----------


