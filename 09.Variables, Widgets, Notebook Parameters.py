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

dbutils.widgets.help()

# COMMAND ----------

# in dropdown the default value must be in the choices list
dbutils.widgets.dropdown("subjectttt","payments", ["subject","payments","sales","orders","transactions"])

# COMMAND ----------

# Subject is the name passed at the side of the Widget
dbutils.widgets.get("subject")

# COMMAND ----------

# In combo you can choose not to add the default value to you  the list of items in your dropdown
dbutils.widgets.combobox("subjectcombo","payments", ["subject","sales","orders","transactions"])

# COMMAND ----------

