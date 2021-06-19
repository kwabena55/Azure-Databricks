# Databricks notebook source
# MAGIC %md
# MAGIC # DBUTILS Python

# COMMAND ----------

# grab all dbutils
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC # List files in Filepath

# COMMAND ----------

# list files in Filepath
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Some Folders in a path

# COMMAND ----------

# Create Some Folders in a path
dbutils.fs.mkdirs("/FileStore/NewFolder")
dbutils.fs.mkdirs("/FileStore/NewFolder/demo1")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/demo1")
dbutils.fs.mkdirs("/FileStore/tables/demo2")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create some files in a Specifc Path

# COMMAND ----------

# create files in a Specific Path
dbutils.fs.put("/FileStore/tables/demo1/solomon.txt","welcome to Solomon Online trainings")

# COMMAND ----------



# COMMAND ----------

dbutils.fs.put("/FileStore/tables/demo2/Ansah.txt","welcome to Ansah Online trainings")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read FileContent/Data stored in Files

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/demo2/Ansah.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC # Move Files

# COMMAND ----------

dbutils.fs.mv("FileStore/tables/demo1/solomon.txt","FileStore/tables/demo2")

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy Files

# COMMAND ----------

dbutils.fs.cp("FileStore/tables/demo2/solomon.txt","FileStore/tables/demo1")

# COMMAND ----------

# MAGIC %md
# MAGIC # Mouting and Unmounting

# COMMAND ----------

#refresh mounts
dbutils.fs.refreshMounts()

# COMMAND ----------

#mounts
dbutils.fs.mounts()

# COMMAND ----------

a=10

# COMMAND ----------

print(a)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exiting Notebook

# COMMAND ----------

dbutils.notebook.exit("testing")

# COMMAND ----------

print(a)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run a notebook from another notebook

# COMMAND ----------

dbutils.notebook.run("/My  Notebooks/07.SQL Notebook  exit",60000) # m60000 means the time to run the notebook . Didnt run becuase of the free subscription

# COMMAND ----------

#use the % run to run the notebook. On the fly it will call all items in that notebook 
%run /MyNotebooks/02.DatabasesandTables_1

# COMMAND ----------

# MAGIC %md
# MAGIC # Install Libraries

# COMMAND ----------

dbutils.library.installPyPI("pandas")

# COMMAND ----------

dbutils.library.installPyPI("tensorflow")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Run and Check some Secrets

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC # Current Notebook Path

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.getContext.notebookPath

# COMMAND ----------

