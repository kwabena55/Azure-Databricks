# Databricks notebook source
# MAGIC %md
# MAGIC # Delete all COntents in a Folder

# COMMAND ----------

dbutils.fs.rm("FileStore/tables/", True)

# COMMAND ----------

dbutils.fs.mkdirs("FileStore/tables/inputavro/")
dbutils.fs.mkdirs("FileStore/tables/outputavro/")

# COMMAND ----------

dbutils.fs.mkdirs("FileStore/tables/inputorc/")
dbutils.fs.mkdirs("FileStore/tables/outputorc/")

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/inputorc/")

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/inputavro/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load/Read ORC Files

# COMMAND ----------

dfavro=spark.read.format("orc").option("header",True).load('dbfs:/FileStore/tables/inputorc/userdata1_orc')

# COMMAND ----------

display(dfavro)

# COMMAND ----------

