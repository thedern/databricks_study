# Databricks notebook source
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
countries_df = spark.read.csv(countries_path, header=True, schema=countries_schema)
countries_df.display()

# COMMAND ----------

# example
# display name values in upper case
# select only returns named column, not whole df
# this data is displayed, not persisted
from pyspark.sql.functions import upper
countries_df.select(upper(countries_df['name'])).withColumnRenamed('upper(name)', 'Name').display()

# COMMAND ----------

# display the length of each country name in an additional column
# notice the header of the additional column will be: 'function(argument)'
from pyspark.sql.functions import length
countries_df.select('name', length(countries_df['name'])).display()

# COMMAND ----------

# get length of name column and display it in an additonal column, but rename header so its not 'function(argument)'
from pyspark.sql.functions import length
countries_df.select('name', length(countries_df['name'])).withColumnRenamed('length(name)', 'NameLength').display()

# COMMAND ----------

'''
Concatinate strings
1. import concat_ws
2. define string concatinator and columns to concatenate
NOTE: the select only returns the named column.  The column header will be named 'function(agrument)', which is not overly readable
'''
from pyspark.sql.functions import concat_ws
countries_df.select(concat_ws(' -- ', countries_df['name'], countries_df['capital'])).display()

# COMMAND ----------

'''
Concatinate strings
1. import concat_ws
2. define string concatinator and columns to concatenate
3. define a column name for readability
'''
from pyspark.sql.functions import concat_ws
countries_df.select(concat_ws(' -- ', countries_df['name'], countries_df['capital'])).withColumnRenamed('concat_ws( -- , name, capital)', 'Name_Capital').display()

# COMMAND ----------


