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
# MAGIC ### Authentication by acess keys

# COMMAND ----------

spark.conf.set("fs.azure.account.key.databrickspratice.dfs.core.windows.net","F8c3iWlMacNl8AjnH7PwSb1p+Ze4U0iZb1g97Q2jVhbNRUuAYvRrCxir8sg3jts2/kYnj25byWoG+AStV711Tg==")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication by SAS Tokens 

# COMMAND ----------


SAS_Token="sp=rl&st=2024-03-31T07:18:52Z&se=2024-03-31T15:18:52Z&spr=https&sv=2022-11-02&sr=c&sig=NczIF4o4vi561I01ViFGtzqnmEApbTL%2FSs01usMUavc%3D"

#spark.conf.set("fs.azure.account.auth.type.databrickspratice.dfs.core.windows.net", "SAS")
#spark.conf.set("fs.azure.sas.token.provider.type.databrickspratice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspratice.dfs.core.windows.net",SAS_Token)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication by service principal

# COMMAND ----------


client_id="82cf32ae-9e2f-404b-b252-74f5fc6e95c8"
tenant_id="4c49cc3b-7d9d-464e-8fc8-a10f91d8117a"
secret_id="Xln8Q~tQXbGem7uQx4Yka4cPc7dbiIOhZqwr-bDu"

spark.conf.set("fs.azure.account.auth.type.databrickspratice.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickspratice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickspratice.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickspratice.dfs.core.windows.net",secret_id )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickspratice.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databrickspratice.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@databrickspratice.dfs.core.windows.net/customersdemo.csv"))

# COMMAND ----------



# COMMAND ----------

dbutils.secrets.help()
dbutils.secrets.list(scope='datalake_scope')


