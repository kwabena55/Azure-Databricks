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

# MAGIC %md
# MAGIC # Write Data as Delta to a Path

# COMMAND ----------

df.write.format("delta").save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Delta Data

# COMMAND ----------

display(spark.read.format("delta").load("dbfs:/FileStore/tables/deltadata/"))

# COMMAND ----------

df1=spark.createDataFrame([(1,"solomonn","Edmonton","AB"),(2,"louisa","calgary","AB"),(3,"Elikem","Chapelle","ED")],["cid","cname","clocation","Province"])

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Facts about Delta and Non Delta tables
# MAGIC * It is observed when we dont write data as delta we can always overwrite the data in the destination folder
# MAGIC * It will never throw an error

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").save("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

