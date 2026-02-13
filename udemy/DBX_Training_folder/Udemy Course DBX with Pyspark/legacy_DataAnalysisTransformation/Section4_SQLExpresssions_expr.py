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

"""
SQL expressions use the are handy for actions you may need but may not be a part of the DF api
If you can do it in SQL, you can do it here too
expr() supports all SQL syntax
"""
from pyspark.sql.functions import expr
countries_df.select(expr('NAME as country_name')).display()

# COMMAND ----------

"""
Example:
Select left characters of Name Column, return as 'left_2_name'
"""
countries_df.select(expr('left(NAME, 2) as left_2_name')).display()

# COMMAND ----------

"""
Add a condition which creates a new column based on population size
NOTE: the double and single quotes and how they are used
NOTE: since this is SQL, 'end' is required at end of statement
"""
countries_df.withColumn('pop_class', expr("case when population > 100000000 then 'large' when population > 50000000 then 'medium' else 'small' end")).display()

# COMMAND ----------

"""
Create a new column 'area_class'
value is 'large' if area_km2 > 1 milliom
value is 'medium' if area_km2 > 300 thousand
else 'small'
"""
countries_df.withColumn('area_class', expr("case when area_km2 > 1000000 then 'large' when area_km2 > 300000 then 'medium' else 'small' end")).display()

# COMMAND ----------


