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

# update df with timestamp column (named timestamp) and save it to persist
from pyspark.sql.functions import current_timestamp
countries_df = countries_df.withColumn('timestamp', current_timestamp())
countries_df.display()

# COMMAND ----------

'''
Get month only from timestamp column
1. import month
2. select month from countries_df, timestamp column
3. rename column, else column header will be named 'function(argument)
'''
from pyspark.sql.functions import month
countries_df.select(month(countries_df['timestamp'])).withColumnRenamed('month(timestamp)', 'month').display()

# COMMAND ----------

'''
Get year only from timestamp column
1. import year
2. select year from countries_df, timestamp column
3. rename column, else column header will be named 'function(argument)
'''
from pyspark.sql.functions import year
countries_df.select(year(countries_df['timestamp'])).withColumnRenamed('year(timestamp)', 'year').display()

# COMMAND ----------

from pyspark.sql.functions import year
'''
if I wish to return the whole df and not select a specific column, I can turn:
    # countries_df.select(year(countries_df['timestamp'])).withColumnRenamed('year(timestamp)', 'year').display()
into the statement below, and use withColumn
'''
countries_df.withColumn('year', year(countries_df['timestamp'])).display()

# COMMAND ----------

'''
Add a string literal column
1. import literal (lit) function
2. add column with literal named header:  'date_literal'
NOTE:  adding a new column using the withColumn method adds supplies a column name in the function.  No need to rename
'''
from pyspark.sql.functions import lit
countries_df = countries_df.withColumn('date_literal', lit('2025-06-16'))
countries_df.display()

# COMMAND ----------

# Since date_literal was persisted, I can select only the 'date_literal' column if I wish
countries_df.select('date_literal').display()

# COMMAND ----------

# verify datatypes
countries_df.dtypes

# COMMAND ----------


'''
cast date_literal column to a date datatype
1. import to_date function
2. add an additiona column 'date', where 'data_literal' is cast to a date, with format of 'year(4)-month(2)-day(2)'
'''
from pyspark.sql.functions import to_date
countries_df = countries_df.withColumn('date', to_date(countries_df['date_literal'], 'yyyy-MM-dd'))
countries_df.display()

# COMMAND ----------

# verify datatypes
countries_df.dtypes

# COMMAND ----------


"""
CONVERT TO TIMESTAMP FROM STRING

convert using to_timestamp function, you can overwrite a column and change its type
create a new df or overwrite existing function
"""
from pyspark.sql.functions import to_timestamp

test_df = countries_df.select('COUNTRY_ID', \
    to_timestamp(countries_df['date_literal'], "yyyy-mm-dd").alias('DL_TIMESTAMP'), \
        'NATIONALITY', \
        'POPULATION', \
        'CAPITAL'
    )
test_df.display()
