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

# add new column, 'withColumn'.  Below, adding a data column to the df
# the new column will be appened to the end of the df
# NOTE: we are not saving the new column back to the dataframe thus it is not persisted, just displayed
from pyspark.sql.functions import curdate
countries_df.withColumn('current_date', curdate()).display()

# COMMAND ----------

# notice current_date column is now missing
countries_df.display()

# COMMAND ----------

"""
1.  add a new column 'pop_in_millions'
2.  data will be population divided by 1,000,000
3.  NOTE: displayed and not persisted
"""
countries_df.withColumn('pop_in_millions', (countries_df.POPULATION/1000000)).display()

# COMMAND ----------

'''
to persist changes for use in subsquent cells, update df by saving
1.  import lit function
2.  create a new column, 'updatedBy' and add the literal 'Darren Smith' to each record
3.  create a new column, 'current_date', and add the current date
4.  create a new column, 'pop_in_millions' and perform simple math to display population data
NOTE: Save on each action
'''
from pyspark.sql.functions import lit
countries_df = countries_df.withColumn('updatedBy', lit('Darren Smith'))
countries_df = countries_df.withColumn('current_date', curdate())
countries_df = countries_df.withColumn('pop_in_millions', (countries_df.POPULATION/1000000))

# COMMAND ----------

# display should show our 3 new columns appended at the end
countries_df.display()

# COMMAND ----------


