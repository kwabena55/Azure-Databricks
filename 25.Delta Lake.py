# Databricks notebook source
# MAGIC %md
# MAGIC # Create DataFrame

# COMMAND ----------

df=spark.createDataFrame([(1,"solomon","Edmonton"),(2,"louisa","calgary"),(3,"Elikem","Chapelle")],["cid","cname","clocation"])

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to DBFS

# COMMAND ----------

df.write.format("parquet").save("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

