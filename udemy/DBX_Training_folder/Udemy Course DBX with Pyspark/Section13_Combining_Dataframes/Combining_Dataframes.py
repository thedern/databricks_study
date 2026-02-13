# Databricks notebook source
# MAGIC %md
# MAGIC ## Dataframe Joins
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html

# COMMAND ----------

"""
Create dataframes for use in this notebook
"""

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_sales = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("sales_amount", IntegerType(), True)
])

data_sales = [
    (1001, 103, '2025-01-15', 5000),
    (1002, 104, '2025-01-16', 7000),
    (1003, 105, '2025-01-17', 6500),
    (1004, 106, '2025-01-18', 4800),
    (1005, 107, '2025-01-19', 5300),
]

schema_stores = StructType([
    StructField("id", IntegerType(), True),
    StructField("store_name", StringType(), True),
    StructField("city", StringType(), True)
])

data_stores = [
    (101, "Store A", "New York"),
    (102, "Store B", "LA"),
    (103, "Store C", "Chicago"),
    (104, "Store D", "Houston"),
    (105, "Store E", "Phoenix"),
]

# create dataframes
sales_df = spark.createDataFrame(data_sales, schema=schema_sales)
stores_df = spark.createDataFrame(data_stores, schema=schema_stores)
sales_df.show()
stores_df.show()

# COMMAND ----------

"""
Use join to combine dataframes
left-join on sales_df, using store id's, preserves all records from the sales_df
join does not need to be imported
"""

sales_df.join(stores_df, sales_df.store_id == stores_df.id, "left").show()

# COMMAND ----------

"""
right join returns all records from the right dataframe, stores_df
"""
sales_df.join(stores_df, sales_df.store_id == stores_df.id, "right").show()

# COMMAND ----------

"""
inner join, return only records where both dataframes match
inner is default and can be omitted as the 3rd argument, but I included it here to be explicity
"""
sales_df.join(stores_df, sales_df.store_id == stores_df.id, 'inner').show()

# COMMAND ----------

"""
'fullouter' join, returns all records from all dataframes
"""
sales_df.join(stores_df, sales_df.store_id == stores_df.id, 'fullouter').show()

# COMMAND ----------

"""
return records from the left dataframe which have NO match in the right dataframe
use 'left_anti' join
"""
sales_df.join(stores_df, sales_df.store_id == stores_df.id, 'left_anti').show()

# COMMAND ----------

"""
there is no 'righ_anti' in spark BUT you can get the same result by swapping the placement of the dataframes in the statement, see below
"""
stores_df.join(sales_df, stores_df.id == sales_df.store_id, 'left_anti').show()

# COMMAND ----------

"""
Cross Join
joins every record of sales_df to every record of stores_df
since there are 5 records in each dataframe, this produces a dataframe with 25 rows
why would you do this?  ... no idea
"""
sales_df.crossJoin(stores_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unioning Dataframes
# MAGIC
# MAGIC Whereas a 'join' integrates table data horizontally on matching columns, 'union' stacks data veritically based shared columns

# COMMAND ----------

"""
create dataframes for union exercise
"""
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

store_schema_1 = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("store_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ]
)

store_schema_2 = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("store_name", StringType(), True),
        StructField("city", StringType(), True)
    ]
)

store_data_1 = [
    (101, "Store A", "New York", "USA"),
    (102, "Store B", "LA", "USA"),
    (103, "Store C", "Chicago", "USA"),
    (104, "Store D", "Houston", "USA"),
    (104, "Store E", "Phoenix", "USA")

]

store_data_2 = [
    (101, "Store A", "New York"),
    (102, "Store B", "LA"),
    (103, "Store C", "Chicago"),
    (104, "Store D", "Houston"),
    (104, "Store E", "Phoenix")
]

stores_df_1 = spark.createDataFrame(store_data_1, schema=store_schema_1)
stores_df_2 = spark.createDataFrame(store_data_2, schema=store_schema_2)
stores_df_1.show()
stores_df_2.show()

# COMMAND ----------

"""
union does not need to imported, its part of the dataframe api
you can union to itself to double your data.  Dunno why would want to do this
"""
stores_df_1.union(stores_df_1).display()

# COMMAND ----------

"""
union does not need to imported, its part of the dataframe api
This will error because the dataframes need the same number of coulums to union
Union does not care about column names, just quantity must match
"""
stores_df_1.union(stores_df_2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Join Exercise

# COMMAND ----------

"""
union does not need to imported, its part of the dataframe api
We need an extra column for the numbers of columns to match
We can 'fudge' this by adding a column to stores_df_2
"""

stores_df_1.union(stores_df_2.select("id", "store_name", "city", "city")).display()

# COMMAND ----------

"""
You can force a match by column name by using 'unionByName'
You can also account for missing columns with this method
Must pass in argument 'allowMissingColumns' which will add 'nulls'
"""

stores_df_1.unionByName(stores_df_2, allowMissingColumns=True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Join Exercise
# MAGIC
# MAGIC NOTE: Records from population table must all be present at the end
# MAGIC 1. read tables into dataframes
# MAGIC 2. clean data if needed
# MAGIC 3. join population and region tables on country_id and id, save to df
# MAGIC 4. on saved df, join sub_region_id on sub-region id
# MAGIC 5. filter unneccessary columns
# MAGIC 6. write to table

# COMMAND ----------

"""
get table data paths
"""
population_path = 'powerplatform_administration_development_2.pyspark_learning.countries_population'
region_path = 'powerplatform_administration_development_2.pyspark_learning.country_regions'
subregion_path = 'powerplatform_administration_development_2.pyspark_learning.country_sub_regions'

# COMMAND ----------

"""
read tables into dataframes
"""
pop_df = spark.read.table(population_path)
reg_df = spark.read.table(region_path)
sub_df = spark.read.table(subregion_path)

# COMMAND ----------

"""
show reg_df
"""
reg_df.display()

# COMMAND ----------

"""
drop null record
"""
reg_df = reg_df.na.drop()
reg_df.display()

# COMMAND ----------

"""
rename name columnn to 'region'
"""
reg_df = reg_df.select(reg_df.id, reg_df.name.alias('region'))
reg_df.display()

# COMMAND ----------

"""
show sub_reg
"""
sub_df.display()

# COMMAND ----------

"""
drop null record
"""
sub_df = sub_df.na.drop()
sub_df.display()

# COMMAND ----------

"""
rename 'name' columnn to 'sub_region'
"""

sub_df = sub_df.select(sub_df.id, sub_df.name.alias('sub_region'))
sub_df.display()

# COMMAND ----------

"""
left join pop_df and reg_df on id
"""
pop_reg_df = pop_df.join(reg_df, pop_df.region_id == reg_df.id, "left")
pop_reg_df.display()

# COMMAND ----------

"""
left join pop_reg_df on sub_df on id
"""
consolidated_df = pop_reg_df.join(sub_df, pop_reg_df.sub_region_id == sub_df.id, "left")
consolidated_df.display()

# COMMAND ----------

df_consolidated_refactored = consolidated_df.select(
    consolidated_df.country_id,
    consolidated_df.name.alias('country'),
    consolidated_df.region,
    consolidated_df.sub_region,
    consolidated_df.population,
    consolidated_df.area_km2
)

df_consolidated_refactored.display()

# COMMAND ----------

"""
create table
"""
df_consolidated_refactored.write.saveAsTable('powerplatform_administration_development_2.pyspark_learning.countries_consolidated', mode="overwrite")

# COMMAND ----------

"""
verify table
"""
spark.read.table('powerplatform_administration_development_2.pyspark_learning.countries_consolidated').display()

# COMMAND ----------

"""
verify via sql
"""
spark.sql('SELECT * FROM powerplatform_administration_development_2.pyspark_learning.countries_consolidated').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clean the null records from my tables permanently so I dont need to deal with it again
# MAGIC DELETE FROM powerplatform_administration_development_2.pyspark_learning.country_regions WHERE id IS NULL;

# COMMAND ----------

spark.read.table('powerplatform_administration_development_2.pyspark_learning.country_regions').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clean the null records from my tables permanently so I dont need to deal with it again
# MAGIC DELETE FROM powerplatform_administration_development_2.pyspark_learning.country_sub_regions WHERE id is NULL;

# COMMAND ----------

spark.read.table('powerplatform_administration_development_2.pyspark_learning.country_sub_regions').display()
