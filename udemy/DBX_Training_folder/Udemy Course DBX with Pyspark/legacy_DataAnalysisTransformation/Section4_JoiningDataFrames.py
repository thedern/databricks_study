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


regions_path = '/FileStore/sample_data_u/country_regions.csv'
regions_schema = StructType([
    StructField("Id", StringType(), False),
    StructField("NAME", StringType(), False)
])
regions_df = spark.read.csv(path=regions_path, header=True, schema=regions_schema)
regions_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Joining:
# MAGIC
# MAGIC Inner - Joins on where dataframes intersect using a common key.  Returns only matching records from both tables
# MAGIC
# MAGIC Left - All rows from the left dataframe are returned along with matching rows from the right table.  There may be rows from left table with Null values were no data matched from the right
# MAGIC
# MAGIC Right - All rows from the right dataframe are returned along with matching rows from the left table.  There may be rows from right table with Null values were no data matched from the left
# MAGIC
# MAGIC Outer - Returns all rows from both dataframes regardless of matching data.  Each row may have Null values depending on data availability

# COMMAND ----------

"""
regions_df['Id'] column corresponds to contries_df['REGION_ID']
Syntax left_df.join(right_df, column == column, join_type)
join_types (inner, outer, right, left)
"""

countries_df.join(regions_df, countries_df['REGION_ID'] == regions_df['Id'], 'inner').display()

# COMMAND ----------

"""
Display only desired columns:
Join dataframes, display only region name, country name, and population columns
alias countries_df['NAME'] as 'country_name;  alias regions_df['NAME'] as 'region_name'
sort desc on population
"""
from pyspark.sql.functions import desc
countries_df.join(regions_df, countries_df['REGION_ID'] == regions_df['Id'], 'inner').\
    select(regions_df['NAME'].alias('region_name'), countries_df['NAME'].alias('country_name'), countries_df['POPULATION']). \
        sort(countries_df['POPULATION'].desc()). \
            display()
