# Databricks notebook source
# Note:  Paths are absolute, not relative, need leading '/'
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

countries_df.display()

# COMMAND ----------

"""
When function is a 'conditional' expression
Below we are adding a column 'name_length' with a text value that indicates country size: "large" or "not large"
Multiple when functions can be chained like the example below.
Syntax when(condition)
Or chained when(condition, value).when(condition, value)
"""
from pyspark.sql.functions import when
countries_df.withColumn('name_length', when(countries_df['population'] > 100000000, 'large').when(countries_df['population'] <= 100000000, 'not large')).display()

# COMMAND ----------

"""
When function is a 'conditional' expression
Below we are adding a column 'name_length' with a text value that indicates country size: "large" or "not large"
For two conditions, multiple when functions can be replaced with 'otherwise', which acts like a logical 'or'
Syntax when(condition)
Or chained when(condition, value).otherwise(value)
NOTE:  otherwise is imported with 'when'
"""
from pyspark.sql.functions import when
countries_df.withColumn('name_length', when(countries_df['population'] > 100000000, 'large').otherwise('not large')).display()

# COMMAND ----------


