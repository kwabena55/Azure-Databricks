# Databricks notebook source
# MAGIC %md
# MAGIC # Read and Write Excel Files

# COMMAND ----------

dbutils.fs.ls("FileStore/tables")

# COMMAND ----------

# Create an input file for Excel Files
dbutils.widgets.text("Filename",".excel")

# COMMAND ----------

dbutils.widgets.get("Filename")

# COMMAND ----------

dbutils.fs.mkdirs("FileStore/tables/excelinput")

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/excelinput")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Excel Files

# COMMAND ----------

data=spark.read.format("com.crealytics.spark.excel").option("header",True).load("/FileStore/tables/excelinput/data.xlsx")

# COMMAND ----------

display(data)

# COMMAND ----------

data.select("country","country_code").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Specific Excel Sheets

# COMMAND ----------

data2=spark.read.format("com.crealytics.spark.excel").option("header",True).option("header",True).option("inferSchema",True).option("dataAddress","'sheet2'!").load("/FileStore/tables/excelinput/data.xlsx")

# COMMAND ----------

display(data2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Specific Cells

# COMMAND ----------

data=spark.read.format("com.crealytics.spark.excel").option("header",True).option("header",True).option("inferSchema",True).option("dataAddress","'sheet2'!A2:C3").load("/FileStore/tables/excelinput/data.xlsx")

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Excel File to a Path

# COMMAND ----------

data.write.format("com.crealytics.spark.excel").option("header",True).save("/FileStore/tables/exceloutput/myexcel.xlsx")

# COMMAND ----------

data.show()

# COMMAND ----------

fileoutput="/FileStore/tables/exceloutput/myexcel.xlsx"
df=spark.read.format("com.crealytics.spark.excel").option("header",True).load(fileoutput)

# COMMAND ----------

df.show()

# COMMAND ----------

fileoutput2="/FileStore/tables/exceloutput/myexcel2.xlsx"
df=data2.write.format("com.crealytics.spark.excel").option("header",True).option("dataAddress","'solosheet'!A1:B1").save(fileoutput2)

# COMMAND ----------

path="/FileStore/tables/exceloutput/myexcel2.xlsx"
data3=spark.read.format("com.crealytics.spark.excel").option("header",True).load(path)
data3.show()

# COMMAND ----------

