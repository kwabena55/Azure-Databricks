# Databricks notebook source
# MAGIC %md
# MAGIC # Read Single File

# COMMAND ----------

# Read Double Pipe Files
# 
df=spark.read.csv("/FileStore/tables/multicsvfile/employeesus.csv", header=True, inferSchema=True,sep="||")

# COMMAND ----------

# Read Files in Databricks
df=spark.read.csv("/FileStore/tables/multicsvfile/employeesus.csv", header=True, inferSchema=True,sep="|")

# COMMAND ----------

# Read Files in Databricks. Dont have a PIPE file so use comma separator please
sourcepath="dbfs:/FileStore/tables/multicsvfile/employeesus.csv"
df=spark.read.csv(sourcepath, header=True, inferSchema=True,sep=",")

# COMMAND ----------

df.show()

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

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Multiple Files in the Source Path

# COMMAND ----------

sourcepath_multiple= "/FileStore/tables/multicsvfile/"
df1=spark.read.csv(sourcepath_multiple, header=True, schema=schema_type,sep=",")


# COMMAND ----------

df1.show()

# COMMAND ----------

df1.describe().show()  # Read alll Files

# COMMAND ----------

