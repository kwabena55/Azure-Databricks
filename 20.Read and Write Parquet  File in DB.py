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

# MAGIC %md
# MAGIC # Partition By

# COMMAND ----------

pardf.write.mode("append").partitionBy("DateID").format("csv").save("FileStore/tables/Parquet/transpart.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Partition Data

# COMMAND ----------

spark.read.csv("/FileStore/tables/Parquet/transpart.csv/DateID=20131224/part-00000-tid-9183230976460932175-bdf97cc0-1554-410a-abdd-2d4fe43d32b8-25-8.c000.csv",header=True).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Database

# COMMAND ----------

pardf.write.format("parquet").saveAsTable("scaladb2.ansahs")

# COMMAND ----------

