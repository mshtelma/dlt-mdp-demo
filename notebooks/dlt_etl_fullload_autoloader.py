# Databricks notebook source
#schema name string, address string, email string, id long, operation string, operation_date timestamp

# COMMAND ----------

from typing import List, Dict, Any
def gen_nn_constraints(fields:List[str])->Dict[str, str]:
  _c = {f"nn_{f}":f"{f} IS NOT NULL" for f in fields}
  _c["corrupt_record"] = "_corrupt_record IS NULL"
  return _c

def gen_q_nn_constraints(fields:List[str])->Dict[str, str]:
  c = " OR ".join([f"({f} IS NULL)" for f in fields])
  c += " OR (_corrupt_record IS NOT NULL)"
  return {'q_nn_constraint':c}

fields = ["name", "address", "email", "operation", "operation_date", "id"]

print(gen_nn_constraints(fields))
print(gen_q_nn_constraints(fields))

# COMMAND ----------


from pyspark.sql.functions import input_file_name, max, col
from pyspark.sql import Window
import dlt 


@dlt.view
def landing_raw_view():
  input_path = spark.conf.get("input_path")
  return (spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .schema("name string, address string, email string, id long, operation string, operation_date timestamp, _corrupt_record string")
            .option("rescuedDataColumn", "_rescued_data")
            .load(input_path)
            .withColumn("file_name", input_file_name())
      )

  
@dlt.table
def landing_raw_history():
  return dlt.read_stream("landing_raw_view")


@dlt.table
def landing_raw():
  df = dlt.read("landing_raw_history")
  df =  (df
        .withColumn('max_file_name', max('file_name').over(Window.orderBy()))
        .where(col('file_name') == col('max_file_name'))
        .drop('max_file_name')
      )
  return df


@dlt.table
@dlt.expect_all_or_drop(gen_q_nn_constraints(fields))
def landing_cleaned():
  return dlt.read("landing_raw")


@dlt.table
@dlt.expect_all_or_drop(gen_q_nn_constraints(fields))
def landing_raw_quarantine():
  return dlt.read("landing_raw")


@dlt.table
@dlt.expect_or_fail("corrupt_or_rescued_records", "q_cnt = 0")
def landing_raw_dropped_threshold():
  return dlt.read("landing_raw_quarantine").selectExpr("count(*) as q_cnt")

# COMMAND ----------


