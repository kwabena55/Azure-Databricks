// Databricks notebook source
// MAGIC %python
// MAGIC # Set the Configuration
// MAGIC # spark.conf.set(
// MAGIC #    "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
// MAGIC #    dbutils.secrets.get(scope="<scope-name>",
// MAGIC #    key="<storage-account-access-key-name>"))

// COMMAND ----------

// MAGIC %python
// MAGIC spark.conf.set("fs.azure.account.key.adlstestg55.dfs.core.windows.net","969+71yPsy6O7riN0kAdTNHqWSMSRBnkLlL9oV6aPJd+btNOq4qUddrQS+4m5Id7xrzyld1P7L8K33UXxxOWqA==")

// COMMAND ----------

// MAGIC %python
// MAGIC #Specify the source path
// MAGIC # sourcepath= spark.read.parquet("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>")

// COMMAND ----------

// MAGIC %python
// MAGIC sourcepath="abfss://datalakecontainer@adlstestg55.dfs.core.windows.net/parquetdata/"

// COMMAND ----------

// MAGIC %python
// MAGIC df=spark.read.parquet(sourcepath)

// COMMAND ----------

