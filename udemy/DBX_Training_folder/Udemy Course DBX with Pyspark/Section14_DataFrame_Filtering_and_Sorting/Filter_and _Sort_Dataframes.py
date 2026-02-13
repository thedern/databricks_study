# Databricks notebook source
# MAGIC %md
# MAGIC ## Filtering Dataframes

# COMMAND ----------

"""
get data for this notebook
"""
consolidated_df = spark.read.table("powerplatform_administration_development_2.pyspark_learning.countries_consolidated")
consolidated_df.display()

# COMMAND ----------

"""
'where' and 'filter' methods, which are part of the API, no need to import
"""
consolidated_df.where(consolidated_df.region == 'Asia').display()

# COMMAND ----------

"""
'filter' syntax is identical
"""
consolidated_df.filter(consolidated_df.region == 'Asia').display()

# COMMAND ----------

"""
sql syntax with filter, notice the quotes and single equals sign '='
"""
consolidated_df.filter("region = 'Asia'").display()

# COMMAND ----------

"""
Multiple conditions '&' or '|'
"""
consolidated_df.filter((consolidated_df.region == 'Asia') | (consolidated_df.region == 'America')).display()

# COMMAND ----------

"""
the same action as above in SQL format which accepts keyword 'or'
"""
consolidated_df.filter("region = 'Asia' or region = 'Europe'").display()

# COMMAND ----------

"""
'and' condition
"""

consolidated_df.filter((consolidated_df.region == 'Asia') & (consolidated_df.country == 'India')).display()

# COMMAND ----------

"""
expression example, where
() indicates order of operations
"""

consolidated_df.where((consolidated_df.population / consolidated_df.area_km2) > 1000).display()

# COMMAND ----------

"""
expression example, filter
() indicates order of operations
"""

consolidated_df.filter((consolidated_df.population / consolidated_df.area_km2) > 1000).display()

# COMMAND ----------

"""
SQL expression syntax for the same operation as above
"""
consolidated_df.where("(population / area_km2) > 1000").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Duplicate Records

# COMMAND ----------

"""
create dataframe
"""
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

dup_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True)
])

dup_data = [
    (1, "Alice", "HR"),
    (2, "Bob", "IT"),
    (3, "Charlie", "Finance"),
    (1, "Alice", "HR"),
    (2, "Bob", "IT"),
    (4, "David", "HR"),
    (3, "Charlie", "Finance"),
    (5, "Alice", "Finance"),
    (6, "Bob", "HR"),
    (2, "Alice", "HR")
]

dup_df = spark.createDataFrame(dup_data, schema=dup_schema)
dup_df.display()

# COMMAND ----------

"""
drop rows where all column data are dupes
"""
dup_df.dropDuplicates().display()


# COMMAND ----------

"""
drop duplicates where 'name' values are the same
to do this, I point the dropduplicates to the 'name' column
dropduplicates when passing args takes a list or tuple
"""
dup_df.dropDuplicates(["name"]).display()

# COMMAND ----------

"""
drop duplicates where 'name' AND 'department' values are the same
dropduplicates when passing args takes a list or tuple
"""
dup_df.dropDuplicates(["name", "department"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting and Limiting records

# COMMAND ----------

"""
get data for this notebook
"""
consolidated_df = spark.read.table("powerplatform_administration_development_2.pyspark_learning.countries_consolidated")
consolidated_df.display()

# COMMAND ----------

"""
sort by population
defaults to ascending
"""
consolidated_df.sort(consolidated_df.population).display()

# COMMAND ----------

"""
descending
"""
consolidated_df.sort(consolidated_df.population, ascending=False).display()

# COMMAND ----------

"""
show 'top 10' records
to do this, use the 'limit' method
"""

consolidated_df.sort(consolidated_df.population, ascending=False).limit(10).display()

# COMMAND ----------

"""
sort by multiple columns
"""
consolidated_df.sort(consolidated_df.region, consolidated_df.population, ascending=False).display()

# COMMAND ----------

"""
asc and desc methods
mix and match!
"""
consolidated_df.sort(consolidated_df.region.asc(), consolidated_df.population.desc()).display()

# COMMAND ----------

"""
orderBy is an alias for sort, does the same thing
"""
consolidated_df.orderBy(consolidated_df.region.asc(), consolidated_df.population.desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering Nulls

# COMMAND ----------

"""
sample data
"""
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

null_data = [
    (1, "Alice", 23),
    (2, None, 30),
    (None, "Bob", None),
    (4, "Charlie", 25),
    (None, None, None)
]

null_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

null_df = spark.createDataFrame(null_data, schema=null_schema)
null_df.display()

# COMMAND ----------

"""
dropna
NOTE:  this is different than 'na.drop()' which I have used before
drop all rows which have ANY nulls
"""
null_df.dropna().display()

# COMMAND ----------

"""
dropna
drop all rows which have at least one 'null' in any column
"""
null_df.dropna(thresh=1).display()

# COMMAND ----------

"""
dropna
drop all rows which have at least two 'null' in any column
"""
null_df.dropna(thresh=2).display()

# COMMAND ----------

"""
provide columns to interrogate
below I will drop all columns which have 'null' in the name col
subset expects a list
"""
null_df.dropna(subset=['name']).display()

# COMMAND ----------

null_df.dropna(subset=['name', 'age']).display()

# COMMAND ----------


