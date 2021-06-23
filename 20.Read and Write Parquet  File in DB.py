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

