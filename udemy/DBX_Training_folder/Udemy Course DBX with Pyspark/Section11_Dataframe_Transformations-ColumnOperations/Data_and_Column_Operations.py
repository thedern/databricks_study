# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Contents
# MAGIC - ## Select by reference and column object
# MAGIC - ## selectExpr()
# MAGIC - ## withColumn()
# MAGIC - ## withColumns()
# MAGIC - ## Renaming Columns - withColumnRenamed(); withColumnsRenamed(); alias()
# MAGIC - ## Changing data types - cast()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Prep & Table Setup

# COMMAND ----------

"""
Import data for use in the notebook exercises
"""

file_path = '/Volumes/powerplatform_administration_development_2/powerplatform_administration/raw_data_files/pyspark/countries_dataset/csv_data/countries_population/countries_population.csv'

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_1 = StructType([
    StructField("country_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("iso_alpha2", StringType(), True),
    StructField("capital", StringType(), True),
    StructField("population", IntegerType(), True),
    StructField("area_km2", IntegerType(), True),
    StructField("region_id", IntegerType(), True),
    StructField("sub_region_id", IntegerType(), True),

])

countries_df = spark.read.csv(file_path, header=True, schema=schema_1)
countries_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a schema, syntax is catalog.schema name
# MAGIC CREATE SCHEMA powerplatform_administration_development_2.pyspark_learning

# COMMAND ----------

"""
From data frame (created via the csv import), create countries_population table in pyspark learning schema
"""
countries_df.write.saveAsTable('powerplatform_administration_development_2.pyspark_learning.countries_population', mode='overwrite')

# COMMAND ----------

"""
verify table contents
remember thats spark.sql returns a dataframe object; therefore
"""
spark.sql("SELECT * FROM powerplatform_administration_development_2.pyspark_learning.countries_population").display()

# COMMAND ----------

"""
Create contry regions table
"""
regions_path = '/Volumes/powerplatform_administration_development_2/powerplatform_administration/raw_data_files/pyspark/countries_dataset/csv_data/country_regions/country_regions.csv'



# COMMAND ----------

"""
Create countries population schema
"""
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

regions_schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True)
    ]
)

# create data frame
regions_df = spark.read.csv(regions_path, schema=regions_schema)

regions_df.display()

# COMMAND ----------

"""
create countries regions table and verify records
"""
# create table
regions_df.write.saveAsTable('powerplatform_administration_development_2.pyspark_learning.country_regions', mode='overwrite')

#verify table
spark.sql("SELECT * FROM powerplatform_administration_development_2.pyspark_learning.country_regions").display()

# COMMAND ----------

"""
Create countries sub regions table
I will do this all in one cell
"""
# set path to data
sub_regions_path = '/Volumes/powerplatform_administration_development_2/powerplatform_administration/raw_data_files/pyspark/countries_dataset/csv_data/country_sub_regions/country_sub_regions.csv'

# create schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sub_regions_schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True)
    ]
)

# create dataframe
sub_regions_df = spark.read.csv(sub_regions_path, schema=sub_regions_schema)

# create table from dataframe
sub_regions_df.write.saveAsTable('powerplatform_administration_development_2.pyspark_learning.country_sub_regions', mode='overwrite')

# verify table records
spark.sql("SELECT * FROM powerplatform_administration_development_2.pyspark_learning.country_sub_regions").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecting Columns
# MAGIC
# MAGIC - select by name returns a reference, and tranformations are not available:  countries_df.select("name")
# MAGIC
# MAGIC - select by brackets returns a column object, transformations are available:  countries_df.select(countries_df["name"])
# MAGIC
# MAGIC - select by dot notation returns a column object, transformations are available:  countries_df.select(countries_df.name)
# MAGIC
# MAGIC - select by col function returns a column object, transformations are available:  countries_df.select(col("name"))
# MAGIC _col is a function which must be imported and takes a column as argument_

# COMMAND ----------

"""
the select method on dataframe is similar to the spark.sql method for returning data. But it's used on dataframes, not tables
"""

# read in countries population table and store in dataframe

countries_df = spark.read.table('powerplatform_administration_development_2.pyspark_learning.countries_population')

# COMMAND ----------

"""
Select by name returns a reference, not a column object.  This means you can display data, but not do data tansformations.
"""
# select by column name
countries_df.select("country_id", "name", "population").display()
# or
# df_selected = countries_df.select("country_id", "name", "population")
# df_selected.display()


# COMMAND ----------

"""
Select ALL columns
"""
countries_df.select("*").display()

# COMMAND ----------

"""
Bracket notation
Requires the name of the dataframe and then column name.
This is very simliar to selecting indexes from a python list object
NOTE:  BRACKET NOTATION RETURNS COLUMN OBJECTS NOT JUST NAME REFERENCES.  Therefore can be used in data transformations
"""
countries_df.select(countries_df['population'], countries_df['name']).display()

# COMMAND ----------

"""
Example transformations using bracket notation
Using dot-notation (method calls) on the bracket notation, one can perform transformations.
Below I am aliasing name to "Country Name"
"""
countries_df.select(countries_df["name"].alias("Country Name")).display()

# COMMAND ----------

"""
Dot notation, like bracket notation, also returns column objects.  Therefore, transformations can be accomplished
This is smoother syntax than bracket notation
"""
countries_df.select(countries_df.name, countries_df.population.alias('POPS!')).display()

# COMMAND ----------

"""
Using the 'col' function.  
Since this is a function, it must be imported (whereas methods are inherent on the objects)
The col function returns column objects
Since it is a function, it takes a column name argument
again, since it returns column objects, transformations can be applied
"""
from pyspark.sql.functions import col
countries_df.select(col("name"), col("population").alias("how many peeps")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column expressions & selectExpr()
# MAGIC can add expressions in with basic select statements

# COMMAND ----------

"""
Expressions require column objects
Below, we will import the 'col' function below to allow for working with column objects
Notice how we can appy operations to column objects using 'col' function
The new column name is named after the expression which created it, see below
"""
from pyspark.sql.functions import col, upper
countries_df.select(
    col("country_id"), 
    upper(col("name")), 
    col("population"), 
    col("area_km2"),
    col("population")/col("area_km2")
).display()

# COMMAND ----------

"""
Using the same operation as above but I am aliasing the new column, creating a more convenient name
"""
from pyspark.sql.functions import col, upper
countries_df.select(
    col("country_id"), 
    upper(col("name")).alias('country-name'), 
    col("population"), 
    col("area_km2"),
    (col("population")/col("area_km2")).alias("population-density")
).display()

# COMMAND ----------

"""
selectExpr() can simplify this using SQL syntax
in the expression below, upper is not the imported upper function, but the SQL syntax supplied 
NOTE:  dashes '-' are illegal in SQL, must use underscores '_'
"""

countries_df.selectExpr(
  "country_id",
  "upper(name) as country_name",
  "population",
  "area_km2",
  "population/area_km2 as population_density"
).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Transformations, withColumn() and withColumns()

# COMMAND ----------

"""
withColumn allows for the transformation of column data, one column at a time
You can replace an existing column or add a column.  Format is (<name>, <operation)
NOTE:  if the column acting upon already exists, then the column will be overwritten with 'new' data.  If the column does not exist, an additonal column will be created.
"""
from pyspark.sql.functions import upper
countries_df.withColumn('name', upper('name')).display()

# COMMAND ----------

"""
Below, since 'country_name does not exist in the dataframe, a new column named country_name was created and added to the end
The code below essentially says, "create a new column named 'country_name' from column 'name'".
"""
countries_df.withColumn('country_name', upper('name')).display()

# COMMAND ----------

"""
Above, all columns are returned along with the additional column.
To select only a subsection of columns, combine the code above with a select statement
NOTE: with column has to come before the select method
"""
countries_df.withColumn('country_name', upper('name')).select('country_name', "country_code", "capital").display()

# COMMAND ----------

"""
withColumn methods can be chained
below I am creating two new columns, then selecting only those columns
"""

countries_df.withColumn('country_name', upper('name')).\
    withColumn("population_density", col("population") / col("area_km2")).\
    select('country_name', 'population_density').\
    display()

# COMMAND ----------



# COMMAND ----------

"""
using withColumns() is more efficient than chaining multiple withColumn() methods
withcolumns() uses a python dict.  Keys are column names, values are the operations
"""

countries_df.withColumns(
    {
    'country_name': upper('name'),
    'population_density': col('population') / col('area_km2')
    }
).select('country_name', 'population_density').display()

# COMMAND ----------

"""
Renaming columns
1. alias()
NOTE:  requies bracket or dot notation as the column object must be available
"""
countries_df.select(countries_df.name.alias("country_name")).\
    select("country_name").display()

# COMMAND ----------

"""
Renaming columns
2. withColumnRenamed()
"""
countries_df.withColumnRenamed('country_code', 'code').withColumnRenamed('country_id', 'id').\
    select('code', 'id').display()

# COMMAND ----------

"""
Renaming Columns
3. withColumnsRenamed()
Like withColumns(), uses a python dict
"""
countries_df.withColumnsRenamed(
    {
        "capital": "main_city",
        "region_id": "region",
        "name": "country"
    }
).select("country", "main_city", "region").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Changing Data Types

# COMMAND ----------


countries_df.dtypes

# COMMAND ----------

"""
cast to different types using python types
use cast method to change data types
below I will case int --> string
Then display the types
This is like an expost-facto schema
"""
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

df_1 = countries_df.select(
    col("country_id").cast(StringType()),
    col("population").cast(StringType())
)
df_1.dtypes

# COMMAND ----------

"""
cast to different types using SQL literals
use cast method to change data types
below I will case int --> string
NOTE: nothing to import
"""
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

df_2 = countries_df.select(
    col("country_id").cast("string"),
    col("population").cast("string")
)
df_2.dtypes
