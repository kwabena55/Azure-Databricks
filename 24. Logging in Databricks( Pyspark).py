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

dir