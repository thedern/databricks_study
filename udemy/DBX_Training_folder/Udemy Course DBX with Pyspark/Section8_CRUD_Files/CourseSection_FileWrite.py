# Databricks notebook source
# create schema
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

# COMMAND ----------

countries_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/csv_data/countries_population/countries_population.csv'

# COMMAND ----------

# read in countries csv for use in writing exercises
countries_df = spark.read.load(countries_path, format='csv', header=True, schema=countries_schema)

# COMMAND ----------

countries_df.display()

# COMMAND ----------

"""
Create an output file path
"""
out_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/output_files/'

# COMMAND ----------

"""
write back to my samples dir
df, when written to csv, creates a set of files inside a folder called countries/
Notice muliple metadata files are created along with the csv data file

NOTE:  the name of the csv created is controlled by the DBX system and cannot be overridden or user selected
The name is a combination of partion id and task id
There are also task 'started' and task 'committend' files

Like read, 'options' are available as well as a 'mode' method
"""
# Examples
countries_df.write.csv(f'{out_path}/countries', header=True, mode='overwrite')
# countries_df.write.mode('overwrite').options(header=True).csv(f'{out_path}/countries')

# COMMAND ----------

"""
Use dbutils to list the countries dir contents
"""
dbutils.fs.ls(f'{out_path}/countries')

# COMMAND ----------

"""
You specifiy the delimiter as well
the default is a comma, but below we are specifying a pipe '|'
"""
countries_df.write.csv(f'{out_path}/countries', header=True, sep='|', mode='overwrite')

# COMMAND ----------

"""
read the contents of the countries dir back into a dataframe
"""

df_back_in = spark.read.csv(f'{out_path}/countries', header=True)
df_back_in.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning Data

# COMMAND ----------

"""
partioning breaks up large datasets for faster processing
I can partition the data by column.
In the example below I can partition the data frame in to regional subfolders under a subfolder named 'regions'
"""
countries_df.write.partitionBy('REGION_ID').csv(f'{out_path}/countries/regions', header=True, mode='overwrite')

# COMMAND ----------

dbutils.fs.ls(f'{out_path}/countries/regions')

# COMMAND ----------

"""
Can also partition and subpartition.  Here we partion by region and then partition region by sub-region
"""
countries_df.write.partitionBy('REGION_ID', 'SUB_REGION_ID').csv(f'{out_path}/countries/regions', header=True, mode='overwrite')

# COMMAND ----------

"""
I can reconstitute the dataframe by loading all csv files in the region subdirectories by referencing the parent dir, 'regions'
See "CourseSection_FileRead" notebook for more details on how this works
"""
df_rebuild = spark.read.csv(f'{out_path}/countries/regions', header=True)
df_rebuild.display()

# COMMAND ----------

"""
We can get a specific region alone by accessing its directory.
NOTE: since there is onlt one csv in the directory, we can pass in the directory name alone.  If there were many csv's and we wanted a specific one, we need to include csv file name in path
"""
df_region_10 = spark.read.csv(f'{out_path}/countries/regions/REGION_ID=10', header=True)
df_region_10.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternate Write Syntax
# MAGIC Once can use the method of the type of file being written (orc, json, parquet, etc).  Below are some examples which use the 'mode', 'format', and file type methods.  Notice the file type examples are the most strait forward.

# COMMAND ----------

## ORC examples
# countries_df_types.write.mode('overwrite').orc(orc_save_path)
# countries_df_types.write.format('orc').save(orc_save_path)
# countries_df_types.write.orc(orc_save_path, mode='overwrite')

## Parquet examples
# countries_df_types.write.mode('overwrite').parquet(parquet_path)
# countries_df_types.write.format('parquet').save(parquet_path)
# countries_df_types.write.parquet(parquet_path, mode='overwrite')
