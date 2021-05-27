# Databricks notebook source
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

display(spark.sql("describe database pythondb"))

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

