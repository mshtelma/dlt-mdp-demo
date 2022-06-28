# Databricks notebook source
# MAGIC  %sh ls /dbfs/mnt/quentin-demo-resources/retail/clients/raw_cdc/

# COMMAND ----------

# MAGIC %sh rm -rf   /dbfs/Users/msh/demo/mdp_dlt/csv_src

# COMMAND ----------

dbutils.fs.mkdirs('/Users/msh/demo/mdp_dlt/csv_src')

# COMMAND ----------

# MAGIC %sh cp /dbfs/mnt/quentin-demo-resources/retail/clients/raw_cdc/part-00000-tid-993673769237771352-83417198-63ba-4a71-8578-e223072968a4-6588-1-c000.csv /dbfs/Users/msh/demo/mdp_dlt/csv_src/f1.csv

# COMMAND ----------

# MAGIC  %sh ls /dbfs/Users/msh/demo/mdp_dlt/csv_src/

# COMMAND ----------

# MAGIC  %sh cp /dbfs/mnt/quentin-demo-resources/retail/clients/raw_cdc/part-00001-tid-993673769237771352-83417198-63ba-4a71-8578-e223072968a4-6589-1-c000.csv /dbfs/Users/msh/demo/mdp_dlt/csv_src/f2.csv

# COMMAND ----------

# MAGIC  %sh ls /dbfs/Users/msh/demo/mdp_dlt/csv_src/

# COMMAND ----------

# MAGIC  %sh cp /dbfs/mnt/quentin-demo-resources/retail/clients/raw_cdc/part-00002-tid-993673769237771352-83417198-63ba-4a71-8578-e223072968a4-6590-1-c000.csv /dbfs/Users/msh/demo/mdp_dlt/csv_src/f3.csv

# COMMAND ----------

# MAGIC  %sh ls /dbfs/Users/msh/demo/mdp_dlt/csv_src/

# COMMAND ----------

# MAGIC  %sh cp /dbfs/Users/msh/demo/mdp_dlt/f4_corrupt.csv /dbfs/Users/msh/demo/mdp_dlt/csv_src/f4_corrupt.csv

# COMMAND ----------

# MAGIC %sh cat  /dbfs/Users/msh/demo/mdp_dlt/csv_src/f4_corrupt.csv

# COMMAND ----------

# MAGIC  %sh ls /dbfs/Users/msh/demo/mdp_dlt/csv_src/

# COMMAND ----------

# MAGIC %sh cp /dbfs/mnt/quentin-demo-resources/retail/clients/raw_cdc/part-00004-tid-993673769237771352-83417198-63ba-4a71-8578-e223072968a4-6592-1-c000.csv /dbfs/Users/msh/demo/mdp_dlt/csv_src/f5.csv

# COMMAND ----------

# MAGIC  %sh ls /dbfs/Users/msh/demo/mdp_dlt/csv_src/

# COMMAND ----------

# MAGIC %sql select * from  msh_dlt_mdp_autoloader.landing_raw

# COMMAND ----------

# MAGIC %sql select * from  msh_dlt_mdp_autoloader.landing_raw_history

# COMMAND ----------

# MAGIC %sql select * from 	msh_dlt_mdp_autoloader.landing_raw_quarantine

# COMMAND ----------

# MAGIC %sql select * from msh_dlt_mdp_autoloader.landing_raw_dropped_threshold

# COMMAND ----------

# MAGIC %sql select * from msh_dlt_mdp_autoloader.curated

# COMMAND ----------

# MAGIC %sql select * from msh_dlt_mdp.landing_raw

# COMMAND ----------


