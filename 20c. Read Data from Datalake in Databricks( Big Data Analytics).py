# Databricks notebook source
# MAGIC %md
# MAGIC # Create an App Registration in Azure and Give permission to Access ADLS
# MAGIC * So basically we are creating an application to access data
# MAGIC * Make sure your applcation will have access to files and folders in your datalake
# MAGIC * Filesystemname variable is the same as the Datalake container name

# COMMAND ----------

  
val appID = ""
val password = ""
val tenantID = ""
val fileSystemName = "";
var storageAccountName = "";

val configs =  Map("fs.azure.account.auth.type" -> "OAuth",
       "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id" -> appID,
       "fs.azure.account.oauth2.client.secret" -> password,
       "fs.azure.account.oauth2.client.endpoint" -> ("https://login.microsoftonline.com/" + tenantID + "/oauth2/token"),
       "fs.azure.createRemoteFileSystemDuringInitialization"-> "true")
dbutils.fs.mount(
source = "abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/",
mountPoint = "/mnt/data",
extraConfigs = configs)


# COMMAND ----------

# MAGIC %md
# MAGIC # Mount Data Lake and Attach it as A Local Drive

# COMMAND ----------



val df = spark.read.csv("/mnt/data/demo/movies.csv")

display(df)

val df = spark.read.option("header", "true").csv("/mnt/data/demo/movies.csv")


val selected = df.select("movieId", "title")

selected.write.csv("/mnt/data/demo/movies-2.csv")