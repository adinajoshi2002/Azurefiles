# Databricks notebook source


# COMMAND ----------

dbutils.fs.help()


# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

for file in dbutils.fs.ls('/'):
    if(file.name.endswith('s/')):
        print(file.name)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets'))
display(dbutils.fs.ls('dbfs:/databricks-datasets/COVID'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore'))
display(spark.read.csv('dbfs:/FileStore/movies.csv'))

# COMMAND ----------

def mount_datalake_acess_keys(Storage_account,Container):

    Secret_access_key = dbutils.secrets.get(scope='datalake_scope',key='DatalakeAccessKey')

    #print( Secret_access_key) 
    mount_point=f"/mnt/{Storage_account}/{Container}"
    try:
        dbutils.fs.mount(
        source = f"wasbs://{Container}@{Storage_account}.blob.core.windows.net",
        mount_point = f"/mnt/{Storage_account}/{Container}",
        extra_configs={f"fs.azure.account.key.{Storage_account}.blob.core.windows.net":Secret_access_key}
       )
        print("Data lake successfully mounted")

    except Exception as e:
        print(f"Error:{str(e)}")
        
    display(dbutils.fs.mounts()) 
    display(dbutils.fs.ls('/mnt/{Storage_account}/{Container}'))
    



# COMMAND ----------

mount_datalake_acess_keys('databrickspratice','raw1')
#display(dbutils.fs.unmount("/mnt/databrickspratice/raw1"))

# COMMAND ----------

Secret_access_key = dbutils.secrets.get(scope='datalake_scope', key='DatalakeAccessKey')
mount_point = "/mnt/databrickspratice/raw"
dbutils.fs.mount(
    source="abfss://raw@databrickspratice.dfs.core.windows.net",
    mount_point=mount_point,
    extra_configs={f"fs.azure.account.key.databrickspratice.dfs.core.windows.net": Secret_access_key}
)
display(dbutils.fs.mounts()) 

# COMMAND ----------

spark.conf.set("fs.azure.account.key.databrickspratice.dfs.core.windows.net","F8c3iWlMacNl8AjnH7PwSb1p+Ze4U0iZb1g97Q2jVhbNRUuAYvRrCxir8sg3jts2/kYnj25byWoG+AStV711Tg==")
#display(dbutils.fs.mounts())
display(dbutils.fs.ls("abfss://raw@databrickspratice.dfs.core.windows.net/")) 

# COMMAND ----------

dbutils.fs.mount(
        source = f"wasbs://raw@databrickspratice.blob.core.windows.net",
        mount_point = f"/mnt/databrickspratice/raw",
        extra_configs={f"fs.azure.account.key.databrickspratice.blob.core.windows.net":dbutils.secrets.get(scope='datalake_scope',key='DatalakeAccessKey')}
)


# COMMAND ----------

display(dbutils.fs.mounts()) 

# COMMAND ----------

display(dbutils.fs.ls("mnt/databrickspratice/raw"))
