# Databricks notebook source
from pyspark.sql.functions import explode, flatten, col, to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
import os
import pandas as pd

# COMMAND ----------

# Get environment variable
environment = os.getenv("ENV")

bronze_restricted_path = "abfss://restricted@dapalphastbronze" + environment + ".dfs.core.windows.net/"
silver_restricted_path = "abfss://restricted@dapalphastsilver" + environment + ".dfs.core.windows.net/"

# COMMAND ----------

main = spark.read.format("parquet").load(bronze_restricted_path + "Capacity_Tracker/Generic/main/*.parquet")
main = main.withColumn("load_date_time", to_timestamp(col("load_date_time"), "yyyyMMddHHmmss"))

vaccinations = spark.read.format("parquet").load(bronze_restricted_path + "Capacity_Tracker/Generic/vaccinations/*.parquet")
vaccinations = vaccinations.withColumn("load_date_time", to_timestamp(col("load_date_time"), "yyyyMMddHHmmss"))

vacancies = spark.read.format("parquet").load(bronze_restricted_path + "Capacity_Tracker/Generic/vacancies/*.parquet")
vacancies = vacancies.withColumn("load_date_time", to_timestamp(col("load_date_time"), "yyyyMMddHHmmss"))

# COMMAND ----------

main.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(silver_restricted_path + "Capacity_Tracker/Generic/main.parquet")
vaccinations.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(silver_restricted_path + "Capacity_Tracker/Generic/vaccinations.parquet")
vacancies.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(silver_restricted_path + "Capacity_Tracker/Generic/vacancies.parquet")
