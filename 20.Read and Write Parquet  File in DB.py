# Databricks notebook source
# MAGIC %md
# MAGIC # Create Some Directories

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/Parquet")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/Parquet_Output")

# COMMAND ----------

pardf=spark.read.parquet("/FileStore/tables/Parquet/NYCTripSmall.parquet")
display(pardf)

# COMMAND ----------

pardf.describe().show()

# COMMAND ----------

pardf.createOrReplaceTempView("sql")

# COMMAND ----------

spark.sql("select count(*) from sql").show()

# COMMAND ----------

