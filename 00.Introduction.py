# Databricks notebook source
# MAGIC %sql
# MAGIC select current_timestamp

# COMMAND ----------

from databricks import koalas as ks

# COMMAND ----------

from pyspark.sql.functions import format_number
from pyspark.sql.types import st

# COMMAND ----------

