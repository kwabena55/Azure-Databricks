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

import pandas as pd

df1 = pd.read_csv("/dbfs/FileStore/shared_uploads/peniel551@outlook.com/appl_stock.csv")

# COMMAND ----------

df1.describe().plot(kind="bar")

# COMMAND ----------

df2 = pd.read_csv("/dbfs/FileStore/shared_uploads/peniel551@outlook.com/airports.csv")

# COMMAND ----------

df2.head()

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_date

# COMMAND ----------

