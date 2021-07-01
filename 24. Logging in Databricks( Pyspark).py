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

