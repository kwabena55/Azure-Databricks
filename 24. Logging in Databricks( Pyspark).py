# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

import logging
logging .debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a Path to store Logs

# COMMAND ----------

import pytz
pytz.all_timezones

# COMMAND ----------

from datetime import datetime
import pytz
current_dt=datetime.now(pytz.timezone('America/Edmonton')).strftime('%Y%m%d_%H%M%S')
directory="/tmp/"
logfilename="solomon"+current_dt+".log"
finalpath=directory+logfilename
print(finalpath)



# COMMAND ----------

import logging
logger=logging.getLogger("demologger")
logger.setLevel(logging.INFO)
FileHandler=logging.FileHandler(finalpath,mode='a')
formatter=logging.Formatter('%(asctime)s - %(name)s -%(levelname)s : %(message)s',datefmt='%m/%d/%Y %I:%M:%S %p')
FileHandler.setFormatter(formatter)
logger.addHandler(FileHandler)
logger.debug('debug message')
logger.info('info message')
logger.warning('warn message')
logger.error('error message')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp

# COMMAND ----------

# MAGIC %md
# MAGIC # Mount and Store Log files in DBFS

# COMMAND ----------

partitions=datetime.now(pytz.timezone('America/Edmonton')).strftime('%Y%m%d_%H%M%S')
print(partitions)

# COMMAND ----------

# MAGIC %md
# MAGIC # Move log files to DBFS

# COMMAND ----------

dbutils.fs.mv("file:"+finalpath,"dbfs:/mnt/solomon/log/"+partitions+logfilename)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Tables in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists solomondefault
# MAGIC using text
# MAGIC options(path 'dbfs:/mnt/solomon/log/',header=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from solomondefault

# COMMAND ----------

# You can mount and write to Blob Storage

# COMMAND ----------

