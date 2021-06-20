# Databricks notebook source
# MAGIC %md
# MAGIC # Pass a single Parameter

# COMMAND ----------

 dbutils.widgets.text("sourcepath","")

# COMMAND ----------

sourcelocation=dbutils.widgets.get("sourcepath")
sourcelocation

# COMMAND ----------

# MAGIC  %md
# MAGIC  # Dropdown : Default value should be in the Choice List

# COMMAND ----------

dbutils.widgets.dropdown("subject_dd","payments", ["sourcepath","payments,","sales","orders","transactions"])

# COMMAND ----------

# Default value must be in choice list, if not use error handling methods
try:
  dbutils.widgets.dropdown("subject_dd","payments", ["sourcepath","payments,","sales","orders","transactions"])
  except:
    print("exception is raised")
  

# COMMAND ----------

