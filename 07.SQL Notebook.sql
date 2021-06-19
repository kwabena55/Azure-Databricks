-- Databricks notebook source
create database sqldatabase

-- COMMAND ----------

desc database sqldatabase

-- COMMAND ----------

create table sqldatabase.mysqltable
(cname string, lname string, salary int)

-- COMMAND ----------

select * from sqldatabase.mysqltable

-- COMMAND ----------

insert into sqldatabase.mysqltable values('solomon','ansah',4000)

-- COMMAND ----------

select * from sqldatabase.mysqltable

-- COMMAND ----------

drop database sqldatabase.mysqltable

-- COMMAND ----------

drop database sqldatabase cascade


-- COMMAND ----------

