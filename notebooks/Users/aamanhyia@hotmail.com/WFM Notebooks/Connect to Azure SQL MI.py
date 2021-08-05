# Databricks notebook source
# MAGIC %md
# MAGIC # Install the MSSQL ODBC Driver

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc |apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msoldbcsql17

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC # Install pyobc

# COMMAND ----------

!pip install pyodbc

# COMMAND ----------

import pyodbc

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Connection to the managed Instance Exposing your Credentials

# COMMAND ----------

try:
  driver='{ODBC Driver 17 for  SQL Server}'
  server="azsqlmi-aatest.public.0369e8e6a188.dtaabase.windows.net"
  database="slmi-database2"
  user="v-anuana"
  password="Forecastexperiment@12345"
  
  query="select * from dbo.student"
  print("query",query)
  conn=pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=3342;DATABASE='+database+';UID='+user+';PWD='+password)
  cursor=conn.cursor()
  cursor.execute(query)
  conn.commit()
  cursor.close()
  conn.close()
except Exception as e:
  print(e)
  raise


# COMMAND ----------

 # Execute this cell to display the widgets on top of the page, then fill the information before continuing to the next cell.
dbutils.widgets.text("hostName", "", "Host Name")
dbutils.widgets.text("database", "", "Database Name")

# COMMAND ----------

