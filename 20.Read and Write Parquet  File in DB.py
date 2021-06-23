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

pardf.count()

# COMMAND ----------

pardf.describe().show()

# COMMAND ----------

pardf.createOrReplaceTempView("sql")

# COMMAND ----------

spark.sql("select count(*) from sql").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # ALternative Syntax

# COMMAND ----------

pardf2=spark.read.format("parquet").load("/FileStore/tables/Parquet/NYCTripSmall.parquet")

# COMMAND ----------

pardf2.collect()[5][0]

# COMMAND ----------

pardf2.dtypes

# COMMAND ----------

pardf2.printSchema() # PrintSchema is more Powerful

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to an Output

# COMMAND ----------

output_path="/FileStore/tables/Parquet_Output"

# COMMAND ----------

pardf.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/Parquet_Output/part-00000-tid-52903537881445447-aed131e0-3cd2-4858-a051-2c3f0194a26a-17-1-c000.snappy.parquet")

# COMMAND ----------

# Reading Data
spark.read.format("parquet").load("/FileStore/tables/Parquet_Output/part-00000-tid-52903537881445447-aed131e0-3cd2-4858-a051-2c3f0194a26a-17-1-c000.snappy.parquet").show()

# COMMAND ----------

