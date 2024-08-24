# Databricks notebook source
# MAGIC %md
# MAGIC ### authentication of datalake to dtaabricks using
# MAGIC 1. acces keys
# MAGIC 2. SAS token
# MAGIC 3. service principal
# MAGIC 4. AAD cerdentials

# COMMAND ----------

print('ny name is joshi')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication by SAS Tokens 

# COMMAND ----------


SAS_Token="sp=rl&st=2024-03-31T07:18:52Z&se=2024-03-31T15:18:52Z&spr=https&sv=2022-11-02&sr=c&sig=NczIF4o4vi561I01ViFGtzqnmEApbTL%2FSs01usMUavc%3D"
spark.conf.set("fs.azure.account.auth.type.databrickspratice.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickspratice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspratice.dfs.core.windows.net",SAS_Token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databrickspratice.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@databrickspratice.dfs.core.windows.net/customersdemo.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##access datalake by use Secrets_scope in databricks
# MAGIC 1. Create Azure key vault and add all secrets key in it for access them from secret scope in databricks. 
# MAGIC 2. Create secret scope in databricks and give azure key vault id.
# MAGIC 3. By using dbutils,secrets we can fetch files from datalake into databricks.

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list(scope='datalake_scope')


# COMMAND ----------

Secret_SAS_Token=dbutils.secrets.get(scope="datalake_scope",key="DatalakeSASToken")

# COMMAND ----------


SAS_Token=dbutils.secrets.get(scope="datalake_scope",key="DatalakeSASToken")
spark.conf.set("fs.azure.account.auth.type.databrickspratice.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickspratice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspratice.dfs.core.windows.net",Secret_SAS_Token)
    



# COMMAND ----------

display(spark.read.csv("abfss://raw@databrickspratice.dfs.core.windows.net/customersdemo.csv"))


# COMMAND ----------


