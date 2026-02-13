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

# MAGIC %md
# MAGIC #### Converting from a Databricks (PySpark) DataFrame to a Pandas DataFrame is often done for specific use cases where the strengths of Pandas are more advantageous than those of PySpark.
# MAGIC
# MAGIC ##### Leveraging Pandas-specific functionalities:
# MAGIC - Pandas offers a vast ecosystem of libraries and functions designed for single-machine data analysis, manipulation, and visualization. 
# MAGIC
# MAGIC - Some operatio ns, especially those involving complex indexing, specific data types (like time series), or advanced plotting, might be more straightforward or efficient to perform directly within Pandas.
# MAGIC
# MAGIC ##### Working with smaller datasets:
# MAGIC - While PySpark excels at distributed processing of large datasets, for smaller datasets that fit comfortably within the memory of a single machine, Pandas can offer faster in-memory processing and a simpler API, reducing the overhead associated with distributed computing.
# MAGIC
# MAGIC ##### Integration with other Python libraries:
# MAGIC - Many Python libraries, particularly those in the machine learning and data science ecosystem, are built to work directly with Pandas DataFrames. Converting to Pandas allows seamless integration with these tools for tasks such as model training, advanced statistical analysis, or custom visualizations.
# MAGIC - Interactive data exploration and prototyping:
# MAGIC
# MAGIC ##### Specific performance considerations for small data:
# MAGIC - For small datasets, the overhead of distributed operations in PySpark can sometimes outweigh the benefits. In such cases, convert

# COMMAND ----------

# convert to pandas df from spark df
# show first 5 records
import pandas as pd
countries_pd = countries_df.toPandas()
countries_pd.head()

# COMMAND ----------

# can use pandas indexing
countries_pd.iloc[0]

# COMMAND ----------


