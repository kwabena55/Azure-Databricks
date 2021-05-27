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

spark.slq