# Databricks notebook source
#schema name string, address string, email string, id long, operation string, operation_date timestamp

# COMMAND ----------

from pyspark.sql.functions import input_file_name
import dlt 

@dlt.view
def landing_raw_view():
  input_path = spark.conf.get("input_path")
  return (spark
      .read
      .format("csv")
      .schema("name string, address string, email string, id long, operation string, operation_date timestamp, _corrupt_record string")
      .option("rescuedDataColumn", "_rescued_data")
      .load(f"{input_path}/f4_corrupt.csv")
      .withColumn("file_name", input_file_name()))

@dlt.table
@dlt.("no_corrupt_records", "_corrupt_record IS NULL")
def landing_raw():
  return dlt.read("landing_raw_view")

@dlt.table
@dlt.expect_or_drop("corrupt_records", "_corrupt_record IS NOT NULL")
def landing_raw_quarantine():
  return dlt.read("landing_raw_view")
