# Databricks notebook source
# Set the Configuration
# spark.conf.set(
#    "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
#    dbutils.secrets.get(scope="<scope-name>",
#    key="<storage-account-access-key-name>"))

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlstestg55.dfs.core.windows.net","969+71yPsy6O7riN0kAdTNHqWSMSRBnkLlL9oV6aPJd+btNOq4qUddrQS+4m5Id7xrzyld1P7L8K33UXxxOWqA==")

# COMMAND ----------

#Specify the source path
# sourcepath= spark.read.parquet("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>")

# COMMAND ----------

sourcepath="abfss://datalakecontainer@adlstestg55.dfs.core.windows.net/parquetdata/"

# COMMAND ----------

df=spark.read.parquet(sourcepath)

# COMMAND ----------

