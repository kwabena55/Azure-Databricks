# Databricks notebook source
# MAGIC %md
# MAGIC # Read and Write Excel Files

# COMMAND ----------

dbutils.fs.ls("FileStore/tables")

# COMMAND ----------

# Create an input file for Excel Files
dbutils.widgets.text("Filename",".excel")

# COMMAND ----------

dbutils.widgets.get("Filename")

# COMMAND ----------

dbutils.fs.mkdirs("FileStore/tables/excelinput")

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/excelinput")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Excel Files

# COMMAND ----------

data=spark.read.format("com.crealytics.spark.excel").option("header",True).load("/FileStore/tables/excelinput/data.xlsx")

# COMMAND ----------

display(data)

# COMMAND ----------

