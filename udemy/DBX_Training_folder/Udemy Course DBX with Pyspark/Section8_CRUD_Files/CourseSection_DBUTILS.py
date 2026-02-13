# Databricks notebook source
# MAGIC %md
# MAGIC ## dbutils
# MAGIC This section focuses mostly on dbutils.fs, which has many useful submethods, such as cp, mv, etc
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# detailed help, use help as a function, not a method.  See the example below
help(dbutils.fs.cp)

# COMMAND ----------

help(dbutils.fs.ls)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/Volumes/powerplatform_administration_development_2/powerplatform_administration/raw_data_files/pyspark/'

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Use dbutils.fs object for file system manipulation

# COMMAND ----------

# delete single file
dbutils.fs.rm('/FileStore/sample_data_u/countries.txt')

# COMMAND ----------

# remove empty folder
dbutils.fs.rm('/FileStore/sample_data')

# COMMAND ----------

# addd key word 'True' for a recursive deletion on a non-empty directory
dbutils.fs.rm('/FileStore/sample_data_u/output', True)

# COMMAND ----------

# make a directory
dbutils.fs.mkdirs('/Volumes/workspace/pyspark_learning/raw_files/test_dir')

# COMMAND ----------

 # copy from one directory to another
 
 dbutils.fs.cp('/Volumes/powerplatform_administration__development_2/powerplatform_administration/raw_data_files/test_dir/test1.txt', '/Volumes/powerplatform_administration__development_2/powerplatform_administration/raw_data_files/test_dir2')

# COMMAND ----------

# move files, recursive
dbutils.fs.mv('/Volumes/powerplatform_administration__development_2/powerplatform_administration/raw_data_files/data_set_users', '/Volumes/powerplatform_administration__development_2/powerplatform_administration/raw_data_files/pbi_data_set_users', recurse=True)

# COMMAND ----------

# rename a file
dbutils.fs.mv('/Volumes/powerplatform_administration__development_2/powerplatform_administration/raw_data_files/text.txt', '/Volumes/powerplatform_administration__development_2/powerplatform_administration/raw_data_files/text2.txt')
