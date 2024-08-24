# Databricks notebook source
# MAGIC %md
# MAGIC ### Cluster_scope_auth
# MAGIC 1. we need to give acess of datalake at cluster level only. 
# MAGIC 2. By opening our created cluster edit configurations by giving end point,acess key.

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databrickspratice.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@databrickspratice.dfs.core.windows.net/customersdemo.csv"))

# COMMAND ----------


