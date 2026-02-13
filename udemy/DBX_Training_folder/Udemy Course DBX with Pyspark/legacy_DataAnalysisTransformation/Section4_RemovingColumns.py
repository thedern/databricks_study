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

# COMMAND ----------

"""
Removing Columns:
If we only need a few columns, it may be best to select and save those columns only
"""
countries_df2 = countries_df.select(countries_df['name'], countries_df['POPULATION'], countries_df['CAPITAL'])
countries_df2.display()

# COMMAND ----------

"""
If we want all but a few columns, drop the cols we dont want
Below, I want everything BUT the ID columns
"""
countries_df_reduced = countries_df.drop(countries_df['ORGANIZATION_REGION_ID'], countries_df['REGION_ID'])
countries_df_reduced.display()
