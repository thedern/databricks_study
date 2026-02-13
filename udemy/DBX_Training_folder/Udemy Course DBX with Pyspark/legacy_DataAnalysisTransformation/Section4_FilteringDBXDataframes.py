# Databricks notebook source
# MAGIC %md
# MAGIC Note:  DBX and Pandas DataFrames are different objects with different methods and capabilites.  

# COMMAND ----------

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

# find all countries with population greater than 1,000,000,000
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html
# https://spark.apache.org/docs/latest/sql-ref-operators.html
countries_df.filter(countries_df['population'] > 1000000000).display()

# COMMAND ----------

# filter and locate
# locate all records where the first letter of the capital city is uppercase 'B'
from pyspark.sql.functions import locate
countries_df.filter(locate("B", countries_df['capital'])==1).display()

# COMMAND ----------

"""
Combined Operations locate THEN a filter
filter by locating all countries with a capital staring with the letter "B" and have a population over 1 billion people
NOTE: column names are not case sensitive
Syntax filter( (locate(condition) & (condition) )   <= notice I am only doing a locate on the first condition and filtering only on the 2nd
https://spark.apache.org/docs/latest/sql-ref-operators.html
"""
countries_df.filter( (locate("B", countries_df['capital'])==1) & (countries_df['population'] > 1000000000) ).display()

# COMMAND ----------

"""
Same a above but using 'Or' condition '|' pipe
Returns countries with captials which start with 'B' or greater than 1 billion people
https://spark.apache.org/docs/latest/sql-ref-operators.html
"""
countries_df.filter( (locate("B", countries_df['capital'])==1) | (countries_df['population'] > 1000000000) ).display()

# COMMAND ----------

"""
Filtering with SQL syntax
Uses simple double quotes
"""
countries_df.filter("region_id == 10").display()

# COMMAND ----------

"""
Filtering with SQL syntax, multiple conditions
Uses simple double quotes
"""
countries_df.filter("region_id == 10 AND population == 0").display()

# COMMAND ----------

"""
Filter records in the countries DF where:
1) name length > 15 characters
2) and region_id is NOT 10
"""
from pyspark.sql.functions import length
countries_df.filter( ( length(countries_df['name']) > 15 ) & (countries_df['region_id'] != 10) ).display()

# COMMAND ----------

"""
Filter records in the countries DF where:
1) name length > 15 characters
2) and region_id is NOT 10
NOTE: Solution below is SQL syntax, which is a little simplier
"""
countries_df.filter("length(name) > 15 AND region_id != 10").display()

# COMMAND ----------


