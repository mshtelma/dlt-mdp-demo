# Databricks notebook source
from pyspark.sql.functions import input_file_name, max, col
from pyspark.sql import Window
import dlt 

@dlt.table
def curated():
  dlt.read("landing_raw_dropped_threshold").count()
  return dlt.read("landing_transformed")
