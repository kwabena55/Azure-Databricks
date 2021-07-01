# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating Databases in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create database using SQL
# MAGIC create database sqldb

# COMMAND ----------

# MAGIC %python
# MAGIC #  Create database using python
# MAGIC spark.sql("create database pythondb")

# COMMAND ----------

# MAGIC %scala
# MAGIC // -- Create database using Scala
# MAGIC spark.sql("create database scaladb")

# COMMAND ----------

# Describe Database in SQL
%sql
describe database sqldb

# COMMAND ----------

# MAGIC %python
# MAGIC # Describe Database in python
# MAGIC spark.sql("describe database pythondb")

# COMMAND ----------

# MAGIC %scala
# MAGIC //  Describe Database in scala
# MAGIC spark.sql("describe database scaladb")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("create database scaladb2 comment 'This is me' ")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("describe database scaladb2")

# COMMAND ----------

# With Comments
spark.sql("create database scaladb3 comment 'Database with comment'")

# COMMAND ----------

spark.sql("describe database scaladb3").show()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("create database pythondb2 comment 'this is a python database' ")

# COMMAND ----------

# Drop Databases
spark.sql("drop database pythondb2")

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("desc database pythondb").show()

# COMMAND ----------

# Create Tables

# COMMAND ----------

spark.sql("create table pythondb.custtable\
         (cid string,\
         cname string,\
         clocation string)")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("create table scaladb.custtable(cid string, cname string,clocation string)")

# COMMAND ----------

spark.sql("select * from scaladb.custtable").show()

# COMMAND ----------

spark.sql("create table scaladb2.custtable ( name string, gender string, age integer)")

# COMMAND ----------

spark.sql("create table scaladb2.custtable2 ( name string,\
          gender string,\
          age integer)")

# COMMAND ----------

# Insert records in Table
spark.sql("insert into scaladb2.custtable values ('solomon','male',1),\
          ('Louisa','female',2)")

# COMMAND ----------

spark.sql("select * from scaladb2.custtable").show()

# COMMAND ----------

spark.sql("describe table scaladb2.custtable").show()

# COMMAND ----------

spark.sql("describe database scaladb2").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from scaladb2.custtable").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("describe table scaladb2.custtable")

# COMMAND ----------

  spark.sql("describe database pythondb2")

# COMMAND ----------

# MAGIC %md
# MAGIC # Show all Temporary and Permanent Tables Under a Database

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("show tables from pythondb2").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Describe Table
# MAGIC * It will show all info about the table

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("desc table pythondb2.permanent_table_name_pat ").show(truncate=false)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from pythondb2.permanent_table_name_pat").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Tables in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists solomondefault
# MAGIC using text
# MAGIC options(path '/mnt/solomon/*/*/*/',header=true)

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy a CSV file or any File to a sql table 

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC create table if not exists customertestt
# MAGIC using csv  //remember you can use text, text etc here 
# MAGIC options(path 'dbfs:/FileStore/tables/customers.csv',header=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customertestt

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists scaladb2.customertestt
# MAGIC using csv 
# MAGIC options(path 'dbfs:/FileStore/tables/customers.csv',header=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe scaladb2.customertestt

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database scaladb2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table scaladb2.customertestt

# COMMAND ----------

