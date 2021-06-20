# Databricks notebook source
# MAGIC %md
# MAGIC # Create Directories First

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/")


# COMMAND ----------

# Create Folders in Filstore PAth
dbutils.fs.mkdirs("FileStore/tables/csvfile/")
dbutils.fs.mkdirs("FileStore/tables/tsvfile/")
dbutils.fs.mkdirs("FileStore/tables/pipefile/")

# COMMAND ----------

# Create Folders in Filstore PAth to store multifiles
dbutils.fs.mkdirs("FileStore/tables/multicsvfile/")
dbutils.fs.mkdirs("FileStore/tables/multitsvfile/")
dbutils.fs.mkdirs("FileStore/tables/multipipefile/")

# COMMAND ----------

 # Create Folders in Filstore PAth
dbutils.fs.ls("FileStore/tables/multicsvfile/")


# COMMAND ----------

# Read Files in Databricks
df=spark.read.csv("/FileStore/tables/multicsvfile/employeesus.csv", header=True, inferSchema=True,sep=",")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Recreate Schema

# COMMAND ----------

