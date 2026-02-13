# Databricks notebook source
# Paths are absolute, not relative, need leading '/'
countries_path = '/FileStore/sample_data_u/countries.csv'
# import data, explictly setting schema and types
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

# math example functions from the pyspark API
# divide
countries_df.select(countries_df['population']/1000000).withColumnRenamed('(population / 1000000)', 'pop_in_millions').display()


# COMMAND ----------

# round a current column to 3 decimals
# can do one math operation at a time
# 1 - make a copy of df to not change original
# 2 - then divide, round, and rename
from pyspark.sql.functions import round
countries_2 = countries_df
countries_2.select(round(countries_2['population']/1000000, 3)).withColumnRenamed('round((population / 1000000), 3)', 'pop_in_millions_rounded').display()

# COMMAND ----------

# in countries_df, create new column, not replace column, called 'population_m'
# 'population_m' sould be 1 decimal place
from pyspark.sql.functions import round
countries_df.withColumn('pop_in_millions', round(countries_df['population']/1000000, 1)).display()

# COMMAND ----------


