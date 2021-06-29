# Databricks notebook source
# Read Files in Databricks
df=spark.read.csv("/FileStore/tables/multicsvfile/employeesus.csv", header=True, inferSchema=True,sep=",")

# COMMAND ----------


# spark=SparkSession.builder.appName("Read CSV Files").getOrCreate Use this when you wowrking local Apache spark
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

schema_type= StructType([ StructField("FirstName",StringType(),True),
                          StructField("Gender",StringType(),True),
                          StructField("StartDate",DateType(),True),
                          StructField("Salary",IntegerType(),True)]
)

# COMMAND ----------

# Read Files in Databricks
df=spark.read.csv("/FileStore/tables/multicsvfile/employeesus.csv", header=True, inferSchema=True,sep=",",schema=schema_type)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Widgets

# COMMAND ----------

dbutils.widgets.text("Enter Source Path", "")

# COMMAND ----------

dbutils.widgets.get("Enter Source Path")

# COMMAND ----------

# MAGIC %md
# MAGIC # Embedding in Code

# COMMAND ----------

path=dbutils.widgets.get("Enter Source Path")

# COMMAND ----------

path

# COMMAND ----------

df3=spark.read.csv(path,header=True,schema=schema_type)
df3.show(n=3)

# COMMAND ----------

# USing DropDown
dbutils.widgets.dropdown("Dropdown","ff",["ff", "employeesus.csv","employeesuk.csv"])

# COMMAND ----------

path1=dbutils.widgets.get("Enter Source Path")
path1

# COMMAND ----------

filename=dbutils.widgets.get("Dropdown")
filename

# COMMAND ----------

# Embedding In code
df4= spark.read.csv(path+filename,header=True)
df4.show(3)

# COMMAND ----------

