# Databricks notebook source
# MAGIC %md
# MAGIC ###### creating mount point code

# COMMAND ----------

##mounting point code
storageAccountName = "databrickspratice"
storageAccountAccessKey = dbutils.secrets.get(scope='datalake_scope',key='DatalakeAccessKey')
sasToken = dbutils.secrets.get(scope='datalake_scope',key='DatalakeSASToken')
blobContainerName = "incrementalload"
mountPoint = "/mnt/databrickspratice/incremental_data/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ######list secrets

# COMMAND ----------

dbutils.secrets.help()
dbutils.secrets.listScopes()
dbutils.secrets.list(scope='datalake_scope')

# COMMAND ----------

# MAGIC %md
# MAGIC #####list mountpoints

# COMMAND ----------

display(dbutils.fs.mounts())
display(dbutils.fs.ls('/mnt/databrickspratice/incremental_data/raw_data'))

# COMMAND ----------

# MAGIC %md
# MAGIC ######fetch widget value,define file pattern

# COMMAND ----------

import re
#dbutils.widgets.text('filename','')
most_recent_file_name=dbutils.widgets.get('filename')
print(most_recent_file_name)
# Define regular expressions for different file types
customer_file_pattern = re.compile(r"^customer_details.*")
product_file_pattern = re.compile(r"^product_details.*")
order_file_pattern = re.compile(r"^order_details.*")
saled_order_file_pattern = re.compile(r"^saled_order_items.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ######reading customers data

# COMMAND ----------

from pyspark.sql.types import*
from pyspark.sql.functions import *
import re
# Check the file type and read data accordingly
if customer_file_pattern.match(most_recent_file_name):
    print(f"Loading customer data from {most_recent_file_name}")
    #filepath
    most_recent_file_path=f'dbfs:/mnt/databrickspratice/incremental_data/customer_details/{most_recent_file_name}'
    #reading new data
    customer_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),  # IDENTITY in SQL is handled differently in Spark
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=False),
    StructField("birth_date", DateType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
    StructField("address", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("points", IntegerType(), nullable=False, metadata={"default": 0}),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
    ])
    df_new = spark.read.format("csv").schema(customer_schema).option("header", "true").load(most_recent_file_path)
    #checking dataframe is empty or not
    if df_new.count() == 0:
        print("The customer_DataFrame is empty.")
    else:
        print("The customer_DataFrame is not empty.")
        c_raw_file_path='dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_customer_details/'

        #reading old data
        df_old=spark.read.format("parquet").schema(customer_schema).option("header", "true").load(c_raw_file_path)

        #identifier to merge data
        count=df_new.count()
        new_id=df_new.agg(min(col('customer_id'))).collect()[0][0]
        old_id=df_old.agg(max(col('customer_id'))).collect()[0][0]
        print(new_id,old_id)
        
        if(new_id==old_id and count>0):
            print('new_id is equal to old_id')
            df_filter_nid_s=df_new.filter(col('customer_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('customer data appended successfuly')

        elif(new_id==(old_id+1) and count>0):
            print('new_id is equal to old_id +1')
            df_new.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('customer data appended successfuly')

        elif(new_id<old_id):
            print('new_id is less than old_id')
            df_filter_nid_s=df_new.filter(col('customer_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('customer data appended successfuly')
    
else:
    print('not customers_data to append')



# COMMAND ----------

 display(df_filter_nid_s)

# COMMAND ----------

# MAGIC %md
# MAGIC ######reading products data

# COMMAND ----------


# Check the file type and read data accordingly
if product_file_pattern.match(most_recent_file_name):
    print(f"Loading product data from {most_recent_file_name}")
    #filepath
    most_recent_file_path=f'dbfs:/mnt/databrickspratice/incremental_data/product_details/{most_recent_file_name}'
    #reading new data
    product_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),  # IDENTITY in SQL is handled differently in Spark
    StructField("name", StringType(), nullable=False),
    StructField("quantity_in_stock", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(4, 2), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
    ])
    df_new = spark.read.format("csv").schema(product_schema).option("header", "true").load(most_recent_file_path)

    #checking dataframe is empty or not
    if df_new.count() == 0:
        print("The customer_DataFrame is empty.")
    else:
        print("The customer_DataFrame is not empty.")
        c_raw_file_path='dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_product_details/'

        #reading old data
        df_old=spark.read.format("parquet").schema(product_schema).option("header", "true").load(c_raw_file_path)

        #identifier to merge data
        count=df_new.count()
        new_id=df_new.agg(min(col('product_id'))).collect()[0][0]
        old_id=df_old.agg(max(col('product_id'))).collect()[0][0]
        print(new_id,old_id)
        
        if(new_id==old_id):
            print('new_id is equal to old_id')
            df_filter_nid_s=df_new.filter(col('product_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('product data appended successfuly')

        elif(new_id==(old_id+1) and count>0):
            print('new_id is equal to old_id+1')
            df_new.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('product data appended successfuly')

        elif(new_id<old_id):
            print('new_id is less than old_id')
            df_filter_nid_s=df_new.filter(col('product_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('product data appended successfuly')
    
else:
    print('not product_data to append')




# COMMAND ----------

# MAGIC %md
# MAGIC ######reading order details

# COMMAND ----------

# Check the file type and read data accordingly
if order_file_pattern.match(most_recent_file_name):
    print(f"Loading order data from {most_recent_file_name}")
    #filepath
    most_recent_file_path=f'dbfs:/mnt/databrickspratice/incremental_data/'\
    f'order_details/{most_recent_file_name}'
    #reading new data
    order_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),  # IDENTITY in SQL is handled differently in Spark
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("order_date", DateType(), nullable=False),
    StructField("status", ShortType(), nullable=False),  # Tinyint maps to ShortType in Spark
    StructField("comments", StringType(), nullable=True),
    StructField("shipped_date", DateType(), nullable=True),
    StructField("shipper_id", ShortType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
    ])
    df_new = spark.read.format("csv").schema(order_schema).option("header", "true").load(most_recent_file_path)

    #checking dataframe is empty or not
    if df_new.count() == 0:
        print("The customer_DataFrame is empty.")
    else:
        print("The customer_DataFrame is not empty.")
        c_raw_file_path='dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_order_details/'

        #reading old data
        df_old=spark.read.format("parquet").schema(order_schema).option("header", "true").load(c_raw_file_path)

        #identifier to merge data
        count=df_new.count()
        new_id=df_new.agg(min(col('order_id'))).collect()[0][0]
        old_id=df_old.agg(max(col('order_id'))).collect()[0][0]
        print(new_id,old_id)

        if(new_id==old_id):
            print('new_id is equal to old_id')
            df_filter_nid_s=df_new.filter(col('order_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('order data appended successfuly')

        elif(new_id==(old_id+1) and count>0):
            print('new_id is equal to old_id+1')
            df_new.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('order data appended successfuly')

        elif(new_id<old_id):
            print('new_id is less than old_id')
            df_filter_nid_s=df_new.filter(col('order_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
            print('order data appended successfuly')
    
else:
    print('not order_data to append')




# COMMAND ----------

# MAGIC %md
# MAGIC ######reading saled order items data

# COMMAND ----------

import re
# Check the file type and read data accordingly
if saled_order_file_pattern.match(most_recent_file_name):
    print(f"Loading sales data from {most_recent_file_name}")
    #filepath
    most_recent_file_path=f'dbfs:/mnt/databrickspratice/incremental_data/'\
    f'saled_order_items/{most_recent_file_name}'
    #reading new data
    saled_order_items_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(6, 2), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
   ])
    df_new = spark.read.format("csv").schema(saled_order_items_schema).option("header", "true").load(most_recent_file_path)

    #checking dataframe is empty or not
    if df_new.count() == 0:
        print("The customer_DataFrame is empty.")
    else:
        print("The customer_DataFrame is not empty.")
        c_raw_file_path='dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_saled_order_items/'

        #reading old data
        df_old=spark.read.format("parquet").schema(saled_order_items_schema).option("header", "true").load(c_raw_file_path)

        #identifier to merge data
        count=df_new.count()
        new_id=df_new.agg(min(col('order_id'))).collect()[0][0]
        old_id=df_old.agg(max(col('order_id'))).collect()[0][0]
        print(new_id,old_id)
        
        if(new_id==old_id):
            print('new_id is equql to old_id')
            df_filter_nid_s=df_new.filter(col('order_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)

        elif(new_id==(old_id+1) and count>0):
            print('new_id is equal to old_id+1')
            df_new.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)

        elif(new_id<old_id):
            print('new_id is less than old_id')
            df_filter_nid_s=df_new.filter(col('order_id')>int(old_id))
            df_filter_nid_s.write.format('parquet').option('header',True).mode('append').save(c_raw_file_path)
    print('saled_order_items data appended successfuly')
else:
    print('not sales_order_items_data to append')



