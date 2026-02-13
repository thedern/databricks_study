# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Contents
# MAGIC - ## Math Functions
# MAGIC - ## String Functions
# MAGIC - ## Date and Time Functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Math Functions
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

# COMMAND ----------

"""
prepare by reading in the countries population table
"""
countries_df = spark.read.table('powerplatform_administration_development_2.pyspark_learning.countries_population')
countries_df.display()

# COMMAND ----------

# DBTITLE 1,Cell 3
"""
Basic math
Example 1
"""
from pyspark.sql.functions import round
# df_updated = df.withColumn("Number", F.round(df["Number"], 2))
countries_df = countries_df.withColumn('population_forcast_2030', round(countries_df.population * 1.2, 2))


# COMMAND ----------

countries_df.display()

# COMMAND ----------

"""
Basic Math
Example 2
"""
countries_df = countries_df.withColumn('population_density', round(countries_df.population / countries_df.area_km2, 2))
countries_df.display()

# COMMAND ----------

"""
greatest and least
"""
from pyspark.sql.functions import greatest, least

countries_df.select(
    "name", 
    "population", 
    "population_forcast_2030", 
    least("population", "population_forcast_2030").alias("lower_of_pop_and_forcast_2030"),
    greatest("population", "population_forcast_2030").alias("greater_of_pop_and_forcast_2030")
    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## String Functions
# MAGIC ### upper and lower
# MAGIC ### lit
# MAGIC ### concat
# MAGIC ### concat_ws

# COMMAND ----------

"""
Count characters with lengh function
"""
from pyspark.sql.functions import length

countries_df.select(
    "name",
    length("name").alias("count_of_chars")
).display()

# COMMAND ----------

"""
Upper and lower
"""
from pyspark.sql.functions import upper, lower

countries_df.select(
    upper("name").alias("NAME"),
    upper("nationality").alias("NATIONALITY")
).display()

# COMMAND ----------

"""
literal data via 'lit'.
Creates column of literal data
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lit.html
"""
from pyspark.sql.functions import lit

countries_df.select("name", lit("Hello"), lit(1).alias('ONE')).display()

# COMMAND ----------

"""
Concatinate
"""
from pyspark.sql.functions import concat

countries_df.select("name", "capital", concat("name", "capital").alias("cap_name")).display()

# COMMAND ----------

"""
As you can see above the capital and name are jammed together
We can do this again using 'lit' to inject a delimiter, in this case comma and a space ", "
"""
countries_df.select("name", "capital", concat("capital", lit(", "), "name").alias("improved_name")).display()

# COMMAND ----------

"""
concat_ws has the same net result as using 'lit' above, but does all within the concat_ws function
format is (separator, column, column)
alias must also be within the concat_ws function
"""
from pyspark.sql.functions import concat_ws
countries_df.select('name', 'capital', concat_ws(", ", "capital", "name").alias('improved_name')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date and Time
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html

# COMMAND ----------

# MAGIC %md
# MAGIC Converting strings to dates and timestamps

# COMMAND ----------

"""
Schema below is SQL syntax, does not use python objects
data is a list of tuples
"""
from pyspark.sql.functions import col

schema = "name string, hire_date string"

data = [
    ("alice", "15-06-2022"),
    ("bob", "25-09-2018"),
    ("charlie", "19-07-2009")
]

df = spark.createDataFrame(data, schema=schema)
df.display()

# COMMAND ----------

"""
Convert the date column from a string to date 
Standard date format is:  yyyy-MM-dd
Stanard timestamp is; yyyy-MM-dd[T]HH:mm:ss[.SSSSSS]
NOTE:  the format input into the to_date function is the current, incorrect format, which it then converts to the standard format
dd-MM-yyyy --> yyyy-MM-dd
NOTE:  not specifying the input format (the incorrect date), will cause an error
"""

from pyspark.sql.functions import to_date

df = df.withColumn("hire_date_converted", to_date(df.hire_date, "dd-MM-yyyy"))
df.display()

# COMMAND ----------

"""
create timestamp with to_timestamp function
"""
from pyspark.sql.functions import to_timestamp

df = df.withColumn('hire_date_to_timestamp', to_timestamp(df.hire_date, "dd-MM-yyyy"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Converting dates and timestamps to strings

# COMMAND ----------

"""
Convert date to string using date_format function
In this case, the format is not the input format, like it is with to_date and to_timestamp, but the output format we want to see
"""
from pyspark.sql.functions import date_format
df = df.withColumn('year', date_format(df.hire_date_converted, 'yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Date and Time Functions
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.curdate.html
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.curdate.html
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.timestamp_diff.html

# COMMAND ----------

"""
Recreate the dataframe with the standard date format for hire_date
"""

"""
Schema below is SQL syntax, does not use python objects
data is a list of tuples
"""
from pyspark.sql.functions import col

schema = "name string, hire_date string"

data = [
    ("alice", "15-06-2022"),
    ("bob", "25-09-2018"),
    ("charlie", "19-07-2009")
]

df = spark.createDataFrame(data, schema=schema)
# overwrite hire_date using withColumn function
df = df.withColumn("hire_date", to_date(df.hire_date, "dd-MM-yyyy"))
df.display()

# COMMAND ----------

"""
Here we are not converting dates/times but adding new columns using the curdate, and current_timestamp functions
These functions take no arguments
NOTE:  hover your mouse over imported functions to get a readme of that function.  Pretty cool.
"""
from pyspark.sql.functions import curdate, current_timestamp

df = df.withColumn("current_date", curdate()).\
  withColumn("current_time_now", current_timestamp())
df.display()

# COMMAND ----------

"""
get the difference in timestamps
Example below gets diff between hire date and current date, in DAYS
Available units:
 “YEAR”, “QUARTER”, “MONTH”, “WEEK”, “DAY”, “HOUR”, “MINUTE”, “SECOND”, “MILLISECOND” and “MICROSECOND”.
"""

from pyspark.sql.functions import timestamp_diff

df.withColumn("days_since_hired", timestamp_diff("DAY", df.hire_date, df.current_date)).display()

# COMMAND ----------

"""
Minutes since hired
"""
df.withColumn("minutes_since_hired", timestamp_diff("MINUTE", df.hire_date, df.current_date)).display()

# COMMAND ----------

"""
Years since hired
"""
df.withColumn("years_since_hired", timestamp_diff("YEAR", df.hire_date, df.current_date)).display()

# COMMAND ----------

"""
Difference between curren_date and current_time_now
"""
df.withColumn("diff_in_times", timestamp_diff("MINUTE", df.current_date, df.current_time_now)).display()

# COMMAND ----------

"""
Add or subtract days, use positive or negative numbers
"""
from pyspark.sql.functions import date_add

df.withColumn("2 additional_days", date_add(df.current_date, 2)).display()

# COMMAND ----------


