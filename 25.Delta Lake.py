# Databricks notebook source
# MAGIC %md
# MAGIC # Create DataFramee

# COMMAND ----------

df=spark.createDataFrame([(1,"solomon","Edmonton"),(2,"louisa","calgary"),(3,"Elikem","Chapelle")],["cid","cname","clocation"])

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to DBFS

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/parquetdata/",True)

# COMMAND ----------

df.write.format("parquet").save("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data as Delta to a Path

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/deltadata/",True)

# COMMAND ----------

df.write.format("delta").save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Delta Data

# COMMAND ----------

display(spark.read.format("delta").load("dbfs:/FileStore/tables/deltadata/"))

# COMMAND ----------

df1=spark.createDataFrame([(1,"solomonn","Edmonton","AB"),(2,"louisa","calgary","AB"),(3,"Elikem","Chapelle","ED")],["cid","cname","clocation","Province"])

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Facts about Delta and Non Delta tables
# MAGIC * It is observed when we dont write data as delta we can always overwrite the data in the destination folder( able to write a 4 column data (df1) to replace a 3 column data(df))
# MAGIC * It will never throw an error
# MAGIC * In the code below we were able to write df1 to the parquet data folder with no error
# MAGIC * Its also evident that when we tried the same thing with the delta it didnt permit us
# MAGIC * It throws an error **A schema mismatch detected when writing to the Delta table**

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").save("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

df1.write.mode("append").format("parquet").save("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/parquetdata/")

# COMMAND ----------

df1.write.mode("overwrite").format("delta").save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

# MAGIC %md
# MAGIC # USe the Merge Schema if you want to append data with different schemma for Delta tables

# COMMAND ----------

df1.write.mode("overwrite").format("delta").option("mergeSchema",True).save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

display(spark.read.format("delta").load("dbfs:/FileStore/tables/deltadata"))

# COMMAND ----------

df.write.mode("append").format("delta").option("mergeSchema",True).save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

display(spark.read.format("delta").load("dbfs:/FileStore/tables/deltadata"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Make a Path a DeltaPath

# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable
dtable=DeltaTable.forPath(spark,"FileStore/tables/deltadata/")

# COMMAND ----------

dtable.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to DF and Read

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Perform Update Operations for DeltaTables

# COMMAND ----------

# Adding more rows
df.write.mode("append").format("delta").option("mergeSchema",True).save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

dtable.update("Province is null",{"Province":"'CY'"}) 

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

# Adding more rows
df.write.mode("append").format("delta").option("mergeSchema",True).save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

dtable.update("cid=2",{"cname":"'newname'"})

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

# Create another DF
updates=spark.createDataFrame([(1,"solomon","Edmont"),(2,"louisaa","calgary"),(3,"Elikem","Chapelle")],["cid","cname","clocation"])

# COMMAND ----------

display(updates)

# COMMAND ----------

# MAGIC %md
# MAGIC # Delete Operations

# COMMAND ----------

dtable.delete("cid=2")

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete multipe conditions

# COMMAND ----------

dtable.delete("Province in ('CY','AB')")

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Delta Table from a Path Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists mydeltadata
# MAGIC using delta
# MAGIC location "dbfs:/FileStore/tables/deltadata/"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mydeltadata

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table mydeltadata rename to mydeltadata1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from mydeltadata1

# COMMAND ----------

# MAGIC %md
# MAGIC # Implementing SCD1 Using SQL in Delta Lake

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

df.write.format("delta").mode("append").option("mergeSchema",True).save("dbfs:/FileStore/tables/deltadata/")

# COMMAND ----------

dtable.toDF().show()

# COMMAND ----------

display(updates)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mydeltadata1

# COMMAND ----------

updates.write.mode("append").format("delta").option("mergeSchema",True).save("dbfs:/FileStore/tables/deltadata2")

# COMMAND ----------

from delta import DeltaTable
dtable2=DeltaTable.forPath(spark,"dbfs:/FileStore/tables/deltadata2")

# COMMAND ----------

dtable2.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a SQL Table for updates

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists updates
# MAGIC using delta
# MAGIC location "dbfs:/FileStore/tables/deltadata2"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from updates

# COMMAND ----------

# MAGIC %sql
# MAGIC update updates
# MAGIC set cname="Louisakolo"
# MAGIC where cid=2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from updates

# COMMAND ----------

