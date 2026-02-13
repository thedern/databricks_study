# Databricks notebook source
# raw python
dbutils.fs.ls("/Volumes/workspace/pyspark_learning/raw_files/pyspark/")

# COMMAND ----------

# MAGIC %md
# MAGIC Line above uses raw pythin dbutils object and its methods
# MAGIC
# MAGIC Line below uses magic %fs method

# COMMAND ----------

# MAGIC %fs ls '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/csv_data/countries_population'

# COMMAND ----------

countries_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/csv_data/countries_population/countries_population.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Method
# MAGIC the examples below use the `spark.read()` method, there is also a `spark.load()`

# COMMAND ----------

'''
Read csv with header option, designating the first row as a header, save to variable
Inspect type using the standard python 'type' function
'''
countries_df = spark.read.csv(countries_path, header=True)
type(countries_df)

# COMMAND ----------

'''
Alternate syntax for reading in data to df with 'options' method.
Options come first, then the file path
I find this to be more verbose, less efficient.
NOTE:  you can pass multiple options using 'options', but only a single option if you use 'option' which can be chained
See the examples below and also notice the syntax difference, key=value for 'options' verus comma delimited for 'option'
'''
# Examples
countries_df2 = spark.read.options(header=True, inferSchema=True).csv(countries_path)
# countries_df2 = spark.read.option('header', True).csv(countries_path)
# countries_df2 = spark.read.option('header', True).option('inferSchema', True).csv(countries_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Method
# MAGIC Displays data in a tabular text format

# COMMAND ----------

"""
by default, truncates data and only shows the top 20 rows.
NOTE: defaults can be overridden
"""
# countries_df.show()
# countries_df.show(truncate=False)
# countries_df.show(n=200)
countries_df.show(truncate=False, n=200)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Method
# MAGIC The `display()` method provides the ability to interact with the data, as if it was a spreadsheet, directly in the cell.  It also allows for the creations visualizations vis the `+` icon in the cell.
# MAGIC
# MAGIC Data is displayed as interactive HTML

# COMMAND ----------

"""
display is a built-in method to dbx, and produces and interactive view.  See '+' symbol at top of table below for interactions
Table can be exported in many file formats
There are icons for search, filter, and explore on the top-right of the table

NOTE:  Profile Data (under the + icon) is only available if you execute display as a method
"""
countries_df.display()

# COMMAND ----------

"""
alternate display syntax, using the display function passing the df as an argument.
this is different than using the 'display' method of the df
df.display() vs display(df)

So display can be called as a method on the dataframe object OR by calling display as a function, passing the dataframe as an argument

NOTE:  Profile Data (under the + icon) is only available if you execute display as a method
"""

display(countries_df2)

# COMMAND ----------

"""
In DBX notebooks, you can also display a data set without saving it to a variable by uing the display function OR display method
"""
# method
spark.read.csv(countries_path, header=True).display()
# function
# display(spark.read.csv(countries_path, header=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema
# MAGIC The cells below compare no schema, inferred schema, and a defined schema

# COMMAND ----------

countries_schema_df = spark.read.csv(countries_path, header=True)
countries_schema_df.display()

# COMMAND ----------

"""
dtypes attribute of the dataframe list the current columns and associated data types.  Note the data was loaded with no schema defined
so everything was interpreted as a 'string' type
"""
countries_schema_df.dtypes

# COMMAND ----------

"""
provides the schema defintion of the dataframe
"""
countries_schema_df.schema

# COMMAND ----------

"""
the describe method 'describes' the schema in a short-hand.  Does not give the schema definition
"""
countries_schema_df.describe()

# COMMAND ----------


"""
To allow for different data types, one can 'infer' the data types.
This allows databricks to determine what the schema should be provided it can identify the data its loading
This is computationally expensive but can work for small projects
"""
countries_schema_df_inferred = spark.read.csv(countries_path, header=True, inferSchema=True)
countries_schema_df_inferred.describe()

# COMMAND ----------

"""
infer is not efficient for large datasets as the data must be read 2x. Once to ingest and once to determine data types.  More efficient to set types manually
1. import the data types from pyspark.sql.types
2. create a schema with defined types(which takes a list).  StuctType function takes a list of StructField objects
3. import data using the schema defined in #2 as an option
Format is:  StructField(<col name>, <datatype>, <required boolean>)
"""
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
countries_df_updated = spark.read.csv(countries_path, header=True, schema=countries_schema)
countries_df_updated.schema
                              

# COMMAND ----------

countries_df_updated.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delimiter
# MAGIC You can manually pass in a delimiter as an argument

# COMMAND ----------

"""
maually set delimiter.  In this case 'tab' (which is incorrect for this file but a good example)
examples below both with and without options
"""
# Examples
# countries_txt = spark.read.csv(countries_path, sep='\t', header=True)
# countries_txt = spark.read.options(header=True,  sep='\t', inferSchema=True).csv(countries_path)
countries_txt = spark.read.option('header', True).option('inferSchema', True).option('sep', '\t').csv(countries_path)
countries_txt.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Method

# COMMAND ----------

'''
the spark load method takes multiple arguments 
this is a very convenient read method as all one needs to do is change the value of the format key, and it handles all file types
format can also be broken out into its own method (format method) as well
to see what methods are available, check the spark API input/output documentation
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html
'''
# Examples
countries_df3 = spark.read.load(countries_path, format='csv', header=True, inferSchema=True)
# countries_df3 = spark.read.format('csv').load(countries_path, header=True)
countries_df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in Multiple Files at Once

# COMMAND ----------

child_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/csv_data/countries_population_partitioned/region_id=10/'
parent_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/csv_data/countries_population_partitioned/'

# COMMAND ----------

"""
looks for all csv files in the folder provided and appends them together
This will grab all files in the folder passed to the child path varable but there is only one file in that directory
Row count = 56
"""
countries_single_df = spark.read.load(child_path, format='csv', header=True, inferSchema=True)
countries_single_df.display()

# COMMAND ----------

"""
looks for all csv files in the folder provided and appends them together
this will grab all files in all child directories
Row count 244
"""

countries_many_df = spark.read.load(parent_path, format='csv', header=True, inferSchema=True)
countries_many_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with JSON

# COMMAND ----------

"""
We will take the current countries df and save as JSON.  
NOTE:  A data frame can be saved as multiple formats
NOTE:  the output of a write action will take the datatype of the data frame.  So for example, if the data frame is all strings, the JSON values written will be all strings
"""
json_save_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/json'
countries_single_df.write.json(json_save_path, mode='overwrite')

# COMMAND ----------

dbutils.fs.ls(json_save_path)

# COMMAND ----------

"""
This JSON output will have many data types aside from string due to the schema when read in
"""

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
countries_df_types = spark.read.csv(countries_path, header=True, schema=countries_schema)
countries_df_types.display()
countries_df_types.write.json(json_save_path, mode='overwrite')

# COMMAND ----------

"""
Reading JSON data into a dataframe
"""
countries_sl_json = spark.read.json(json_save_path)
countries_sl_json.display()

# COMMAND ----------

"""
Notice the schema is slightly changed from how I read in the data from csv and created the 'countries_df_types', data_frame
When saved as JSON, I saved int, double, and string;  however,  that schema does not get saved in JSON
JSON recognizes numbers and strings. So when read in the JSON, I get numbers and strings UNLESS I reapply a more specific schema
"""
countries_sl_json.dtypes

# COMMAND ----------

# import muli-line json, all objects formatted as distinct objects in an array (list) within the json file.
# will error on display if 'multi-line' not set to True
countries_ml_json = spark.read.json('/FileStore/sample_data_u/countries_multi_line.json', multiLine=True)
countries_ml_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORC Data Files
# MAGIC Open source format where schema is saved with data file. ORC file is saved as binary on the file system

# COMMAND ----------

"""
Using the typed dataframe from the examples above we can write data in the ORC format
NOTE: you cannot view an ORC file contents in the volume like you can text, csv, json.  Its binary encoded
"""
orc_save_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/orc_files'
# countries_df_types.write.mode('overwrite').orc(orc_save_path)
# countries_df_types.write.format('orc').save(orc_save_path)
countries_df_types.write.orc(orc_save_path, mode='overwrite')

# COMMAND ----------

"""
Read orc
Again, there are multiple ways to do this.  Below I am not saving the dataframe at read, just displaying it
"""
# spark.read.load(orc_save_path, format='orc').display()
spark.read.orc(orc_save_path).display()

# COMMAND ----------

"""
Notice the schema was saved with the data file so I did not have to apply a schema when reading it in like I would csv or JSON
"""
orc_df = spark.read.orc(orc_save_path)
orc_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet
# MAGIC Like ORC, this is stored as binary with the schema in the file, no need to apply schema when loading

# COMMAND ----------

"""
Reusing our schema applied data frame from above 'countries_df_types', we will save the dataframe as parquet

Many ways to write this using both the 'save' and 'parquet' methods
"""
parquet_path = '/Volumes/workspace/pyspark_learning/raw_files/pyspark/countries_dataset/parquet_files'
# countries_df_types.write.mode('overwrite').parquet(parquet_path)
# countries_df_types.write.format('parquet').save(parquet_path)

# I like this one
countries_df_types.write.parquet(parquet_path, mode='overwrite')

# COMMAND ----------

"""
Reading parquet files
Again, multiple ways to load
"""
# parquet_df = spark.read.format('parquet').load(parent_path)
parquet_df = spark.read.parquet(parquet_path)
parquet_df.display()
parquet_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## DeltaLake Format
# MAGIC Advaned file format, the schema is contained within the file asd it is stored as parquet.  Delta builds on parquet and adds additional features such as ACID transactions
# MAGIC
# MAGIC NOTE: Delta is the defualt format for databricks tables
# MAGIC

# COMMAND ----------

"""
Once again we will use our 'countries_df_types' data frame as a starting point

NOTE:  there is no detlalake method, so must use the write.format.mode.save method syntax
"""
delta_save_path= ('/Volumes/workspace/pyspark_learning/raw_files/countries_dataset/detaLake')
countries_df_types.write.format('delta').mode('overwrite').save(delta_save_path)
dbutils.fs.ls(delta_save_path)

# COMMAND ----------

"""
Read Delta
I am using the load method as there is no 'detalake' method
"""
spark.read.load(delta_save_path, format='delta').display()
