# Databricks notebook source
driver_day1_df = spark.read.option("inferschema", True).json("/mnt/formual1dl/raw/2021-03-28/drivers.json").filter("driverId <= 10")\
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location "/mnt/formual1dl/demo"

# COMMAND ----------

display(driver_day1_df)

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day2_df = spark.read.option("inferschema", True).json("/mnt/formual1dl/raw/2021-03-28/drivers.json").filter("driverId between 6 and 15")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day2_df)

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

driver_day3_df = spark.read.option("inferschema", True).json("/mnt/formual1dl/raw/2021-03-28/drivers.json").filter("driverId between 1 and 5 or driverId between 16 and 20")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day3_df)

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC Using Delta

# COMMAND ----------

