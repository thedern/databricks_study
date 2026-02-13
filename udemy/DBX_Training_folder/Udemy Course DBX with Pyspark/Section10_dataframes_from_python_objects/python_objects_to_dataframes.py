# Databricks notebook source
"""
list of lists
"""
data_1 = [
    ["alice", 30, "london"],
    ["bob", 25, "new york"],
    ["carol", 27, "san francisco"],
    ["dave", 35, "berlin"],
]

# COMMAND ----------

"""
create a dataframe with no column mames
"""
df_no_name = spark.createDataFrame(data_1)
df_no_name.display()

# COMMAND ----------

"""
create a dataframe with column names by adding a schema
NOTE: the schema below is just column names, not data types.  DBX will infer the types
"""

df_col_names = spark.createDataFrame(data_1, schema=["name", "age", "city"])
df_col_names.display()

# COMMAND ----------

"""
list of dictionaries
"""
data_2 = [
    {"name": 'alice', 'age': 30, 'city': 'london'},
    {"name": 'bob', 'age': 25, 'city': 'new york'},
    {"name": 'carol', 'age': 27, 'city': 'san francisco'},
    {"name": 'dave', 'age': 35, 'city': 'berlin'},
]

# COMMAND ----------

"""
Below creates a 'typed' schema using ddl formating
"""
schema_1 = "name string, age integer, city string"
df_col_names_2 = spark.createDataFrame(data_1, schema=schema_1)
df_col_names_2.display()

# COMMAND ----------

"""
With a dict, the elements are key/value pairs so column names are determined by createDataFrame
NOTE: dbx will infer the types as no schema is provided
"""
df_dict= spark.createDataFrame(data_2)
df_dict.display()

# COMMAND ----------


"""
You can convert any python data type to a data frame, even a tuple
"""
tuple_challenge = ((54, 'dave'), (18,'sandy'), (90, 'ed'))
print(type(tuple_challenge))
schema_2 =  "age integer, name string"
spark.createDataFrame(tuple_challenge, schema=schema_2).display()
