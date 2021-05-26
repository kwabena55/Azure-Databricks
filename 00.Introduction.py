# Databricks notebook source
# MAGIC %sql
# MAGIC select current_timestamp

# COMMAND ----------

from databricks import koalas as ks

# COMMAND ----------

from pyspark.sql.functions import format_number
from pyspark.sql.types import st

# COMMAND ----------

df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/peniel551@outlook.com/airports.csv",header=True)

# COMMAND ----------

df1.take(5)

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.createOrReplaceTempView('SQL')

# COMMAND ----------


spark.sql ("Select * from sql").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df1

# COMMAND ----------

