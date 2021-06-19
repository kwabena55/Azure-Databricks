// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC  # var -- mutable -- Changeable
// MAGIC # val -- immutable -- Unchangeable

// COMMAND ----------

spark.sql("create database scaladb5")

// COMMAND ----------

var df= spark.sql("desc database scaladb5")  // this mean you have assigned it to a variable that can be reassigned

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Add Comments

// COMMAND ----------

spark.sql("create database scaladb6 comment 'this is a new db with comments' ")

// COMMAND ----------

var df= spark.sql("desc database scaladb6")
display(df)

// COMMAND ----------

spark.sql("create table scaladb6.scalatable6666 (cfname string,clname string,salary int )")

// COMMAND ----------

spark.sql("select * from scaladb6.scalatable6666").show()

// COMMAND ----------

spark.sql("insert into scaladb6.scalatable6666 values ('solomon','ansah',20000)")

// COMMAND ----------

spark.sql( "select * from scaladb6.scalatable6666").show()

// COMMAND ----------

// MAGIC %md 
// MAGIC #Drop Database with data using Cascade

// COMMAND ----------

spark.sql( "Drop database scaladb6 cascade")

// COMMAND ----------

spark.sql( "show tables from scaladb6")

// COMMAND ----------

