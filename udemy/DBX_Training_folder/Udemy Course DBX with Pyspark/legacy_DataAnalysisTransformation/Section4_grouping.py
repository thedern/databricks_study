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

# MAGIC %md
# MAGIC NOTE:  I had some odd errors with aggregations and recognizing the correct datatype wich went away if I used the UPPERCASE column names as indicated in teh schema.  Lowercase, results varried, no idea why

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

"""
groupBy method creates a group object
"""
countries_df.groupBy(countries_df['REGION_ID'])

# COMMAND ----------

"""
to display grouped data, we must perform an aggregation on the group
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions
"""

from pyspark.sql.functions import sum
countries_df.groupBy('REGION_ID').sum('POPULATION').display()

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE:  index notation like: 
# MAGIC
# MAGIC countries_df.groupBy(countries_df['REGION_ID']).sum(countries_df['POPULATION']).display() ... generates an 'ASSERTION ERROR'
# MAGIC
# MAGIC The aggregation expects strings like: 
# MAGIC
# MAGIC countries_df.groupBy('REGION_ID').sum('POPULATION').display()
# MAGIC
# MAGIC I believe this is because groupBy and sum take string arguments only, indexes cause an error

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

"""
Get the min
"""
from pyspark.sql.functions import min
countries_df.groupBy('REGION_ID').min('POPULATION').display()

# COMMAND ----------

"""
Get the average
And split code over multiple lines using '\'
"""
from pyspark.sql.functions import avg
countries_df. \
groupBy('REGION_ID'). \
avg('POPULATION').display()

# COMMAND ----------

"""
Get the sum of multiple columns
 split code over multiple lines using '\'
"""

countries_df. \
groupBy('REGION_ID'). \
sum('POPULATION', 'AREA_KM2').display()

# COMMAND ----------

"""
Multiple aggregations at one time require the 'agg' method
Below we are getting the average of the population and the sum of the km2
the 'agg' function takes other functions and their arguments as args
Syntax agg(func(arg), func(arg))
"""
from pyspark.sql.functions import avg, sum
countries_df.groupBy('REGION_ID').agg(avg('POPULATION'), sum('AREA_KM2')).display()

# COMMAND ----------

"""
Group by muliple columns
NOTE:  To sort, I have to return to index notation as I need to apply the 'asc()' method to column directly.  Only index notation supports methods.
In contrast groupBy, agg, avg, and sum, are all functions which take arguments and are not methods on a column
"""
countries_df.groupBy('REGION_ID','SUB_REGION_ID').agg(avg('POPULATION'), sum('AREA_KM2')).sort(countries_df['REGION_ID'].asc()).display()

# COMMAND ----------

"""
The code below has the same data output as the code above but we are aliasing the column names, then sorting
NOTE:  I am using the withColumnRenamed function and not alias method,
        as I am not selecing columns by index but passing in column names as arguments
"""

countries_df.groupBy('REGION_ID','SUB_REGION_ID'). \
    agg(avg('POPULATION'), sum('AREA_KM2')). \
        withColumnRenamed('avg(POPULATION)', 'ave_pop'). \
            withColumnRenamed('sum(AREA_KM2)', 'total_area'). \
                sort(countries_df['REGION_ID'].asc()). \
                    display()

# COMMAND ----------

"""
Alias example which is a method on the new column name, and not a function to which the column name is passed (withColumn)
This is less code, same result
"""

countries_df.groupBy('REGION_ID','SUB_REGION_ID'). \
    agg(avg('POPULATION').alias('ave_pop'), sum('AREA_KM2').alias('total_area')). \
                sort(countries_df['REGION_ID'].asc()). \
                    display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

"""
Aggregate the dataframe by region_id and sub_region_id
Display the minimum and maximum population aliased by max_pop, min_pop
sort by region_id asc

NOTE:  that i use index naming to sort as 'asc()' is a method on the DF column.
"""
from pyspark.sql.functions import min, max
countries_df.groupBy('REGION_ID', 'SUB_REGION_ID'). \
    agg(min('POPULATION').alias('min_pop'), max('POPULATION').alias('max_pop')). \
        sort(countries_df['REGION_ID'].asc()).display()

