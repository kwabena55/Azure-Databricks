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

com.crealytics:spark-excel_2.12:0.13.7
    /FileStore/tables/excelinput/data__2_.xlsx'
