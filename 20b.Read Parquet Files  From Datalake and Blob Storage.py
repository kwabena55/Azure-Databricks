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

# %python
blobAccesskey = dbutils.secrets.get(scope ="myblob", key= "accesskey")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # Configuration to Access Storage Account

// COMMAND ----------

// val blobStorage = "<blob-storage-account-name>.blob.core.windows.net"
// val blobContainer = "<blob-container-name>"
// val blobAccessKey =  "<access-key>"

// COMMAND ----------

val StorageAccountName = "storageblob55"
val blobContainer = "blobcontainer"
val blobAccessKey = dbutils.secrets.get(scope ="myblob", key= "accesskey")

// COMMAND ----------

spark.conf.set( "fs.azure.account.key." + StorageAccountName + ".dfs.core.windows.net",blobAccessKey)

// COMMAND ----------

// MAGIC %md
// MAGIC # Read Data from Storage Account

// COMMAND ----------

val filename = "NYCTripSmall.parquet"
val data=spark.read.parquet("wasbs://" + blobContainer + "@" + StorageAccountName + ".blob.core.windows.net/" + filename)

// COMMAND ----------

display(data)

// COMMAND ----------

val filename = "employees.csv"
val data=spark.read.option("header","true").csv("wasbs://" + blobContainer + "@" + StorageAccountName + ".blob.core.windows.net/" + filename)

// COMMAND ----------

display(data)

// COMMAND ----------

// MAGIC %md
// MAGIC #Read Data from Datalake

// COMMAND ----------



// COMMAND ----------

val StorageAccountName = "storagedatalake55"
val datalakeContainer = "datalakecontainer"
val blobAccessKey2 = dbutils.secrets.get(scope ="myadls", key= "accesskey2")

// COMMAND ----------

spark.conf.set( "fs.azure.account.key." + StorageAccountName + ".dfs.core.windows.net",blobAccessKey2)


// COMMAND ----------

filename2="NYCTripSmall.parquet"
sourcepath= spark.read.parquet("abfss://" + datalakeContainer + "@" + StorageAccountName + ".dfs.core.windows.net/parquetdata/" +filename2)