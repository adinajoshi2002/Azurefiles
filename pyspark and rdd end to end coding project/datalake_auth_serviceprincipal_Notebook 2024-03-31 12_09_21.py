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
# MAGIC ## Authentication by service principal

# COMMAND ----------


client_id="82cf32ae-9e2f-404b-b252-74f5fc6e95c8"
tenant_id="4c49cc3b-7d9d-464e-8fc8-a10f91d8117a"
client_secret="Xln8Q~tQXbGem7uQx4Yka4cPc7dbiIOhZqwr-bDu"

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

# MAGIC %md
# MAGIC ## Access datalake by using secret scope for service principal auth

# COMMAND ----------

dbutils.secrets.list(scope="datalake_scope")

# COMMAND ----------

Secret_client_id = dbutils.secrets.get(scope="datalake_scope",key="datalakeServicePrinClientID")
Secret_tenant_id = dbutils.secrets.get(scope="datalake_scope",key="datalakeServicePrinTenantID")
Secret_client_secret = dbutils.secrets.get(scope="datalake_scope",key="datalakeServicePrinClientScrete")

spark.conf.set("fs.azure.account.auth.type.databrickspratice.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickspratice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickspratice.dfs.core.windows.net", Secret_client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickspratice.dfs.core.windows.net",Secret_client_secret )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickspratice.dfs.core.windows.net", f"https://login.microsoftonline.com/{Secret_tenant_id}/oauth2/token")

# COMMAND ----------

display(spark.read.csv("abfss://raw@databrickspratice.dfs.core.windows.net/customersdemo.csv"))

# COMMAND ----------


