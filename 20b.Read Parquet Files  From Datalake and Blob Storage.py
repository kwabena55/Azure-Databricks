// Databricks notebook source
// MAGIC %python
// MAGIC # Set the Configuration
// MAGIC # spark.conf.set(
// MAGIC #    "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
// MAGIC #    dbutils.secrets.get(scope="<scope-name>",
// MAGIC #    key="<storage-account-access-key-name>"))

// COMMAND ----------

// # %python
// # spark.conf.set("fs.azure.account.key.adlstestg55.dfs.core.windows.net","969+71yPsy6O7riN0kAdTNHqWSMSRBnkLlL9oV6aPJd+btNOq4qUddrQS+4m5Id7xrzyld1P7L8K33UXxxOWqA==")

// COMMAND ----------

# %python
#Specify the source path
# sourcepath= spark.read.parquet("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>")

// COMMAND ----------

# %python
# sourcepath="abfss://datalakecontainer@adlstestg55.dfs.core.windows.net/parquetdata/"

// COMMAND ----------

# %python
# df=spark.read.parquet(sourcepath)

// COMMAND ----------

// MAGIC %md
// MAGIC # Secret Scope
// MAGIC * A secret scope and key has been created in Databricks CLI
// MAGIC * The access key of the storage account was grabbed and put in the Secret Scope
// MAGIC * It means the secret scope being a logical container is storing all the keys of my storage account which can be levraged in my code

// COMMAND ----------

// MAGIC %python
// MAGIC blobAccesskey = dbutils.secrets.get(scope ="myblob", key= "accesskey")

// COMMAND ----------

