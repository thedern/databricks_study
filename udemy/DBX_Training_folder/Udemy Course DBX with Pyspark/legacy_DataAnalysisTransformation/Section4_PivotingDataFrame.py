# Databricks notebook source
# Paths are absolute, not relative, need leading '/'
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
Notice how group and summatin displays 3 columns.  The region IDs are in the first column, sub-region ID is in the 2nd column
"""
from pyspark.sql.functions import sum, asc
countries_df.groupBy('REGION_ID', 'SUB_REGION_ID').sum('POPULATION').sort(countries_df['REGION_ID'].asc()).display()

# COMMAND ----------

"""
In contrast to above, I will pivot the DF so each sub-region ID is its own column
"""
countries_df_pivoted = countries_df.groupBy('REGION_ID').pivot('SUB_REGION_ID').sum('POPULATION').sort(countries_df['REGION_ID'].asc())
countries_df_pivoted.display()

# COMMAND ----------

"""
Unpivot uses the 'stack' function from expr
"""
from pyspark.sql.functions import expr
unpivot = countries_df_pivoted.select('REGION_ID', expr("stack(18, 'null', null, '10', 10, '20', 20, '30', 30, '40', 40, '50', 50, '60', 60, '90', 90, '100', 100, '110', 110, '120', 120, '130', 130, '140', 140, '150', 150, '160', 160, '170', 170) as (REGION_ID, POPULATION)"))
unpivot.display()
