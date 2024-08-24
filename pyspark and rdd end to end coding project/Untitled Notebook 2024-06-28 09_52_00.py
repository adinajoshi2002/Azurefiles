# Databricks notebook source

from pyspark.sql.types import *
data=[(1,2),(3,4),(4,5)]
schema=StructType([StructField('num1',IntegerType()),StructField('num2',IntegerType())])
df=spark.createDataFrame(data=data,schema=schema)
df.show()
df.write.format('parquet').mode('overwrite').option('header','true').save('/FileStore/tables/data/')

# COMMAND ----------

#dbutils.fs.ls('/FileStore/tables/data/')
dbutils.fs.rm('/FileStore/tables/data',True)

# COMMAND ----------

from pyspark.sql.functions import *
df_new=spark.read.format('parquet').option('header','true').load('/FileStore/tables/data/')
display(df_new)
#print(df_new.agg(max(col('num1'))).collect()[0][0])


# COMMAND ----------

from pyspark.sql.types import *
data=[(11,12),(13,14),(14,15)]
schema=StructType([StructField('num1',IntegerType()),StructField('num2',IntegerType())])
df_append=spark.createDataFrame(data=data,schema=schema)
df_version=df_new.unionByName(df_append)
df_version.show()
df_version.write.format('parquet').mode('overwrite').option('header','true').save('/FileStore/tables/data/')

# COMMAND ----------

from pyspark.sql.types import *
data=[(200,200)]
schema=StructType([StructField('num1',IntegerType()),StructField('num2',IntegerType())])
df_append=spark.createDataFrame(data=data,schema=schema)
df_append.show()
df_version=df_new.unionByName(df_append)
#df_version.show()
df_append.write.format('parquet').mode('append').option('header','true').save('/FileStore/tables/data/')

# COMMAND ----------

from pyspark.sql.types import *
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
df_new = spark.read.format("csv").schema(customer_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/customer_details/customer_details_2024-06-27T12:47:33.2941417Z.csv')
df_new.show()
df_new.write.format('parquet').option('header',True).mode('overwrite').save('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_customer_details')

# COMMAND ----------

from pyspark.sql.types import *
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
df=spark.read.format("parquet").schema(customer_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_customer_details/')
display(df)

# COMMAND ----------

from pyspark.sql.types import *
# Define the schema
product_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),  # IDENTITY in SQL is handled differently in Spark
    StructField("name", StringType(), nullable=False),
    StructField("quantity_in_stock", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(4, 2), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
])
df_new = spark.read.format("csv").schema(product_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/product_details/product_details_2024-06-27T12:48:06.2302178Z.csv')
df_new.show()
df_new.write.format('parquet').option('header',True).mode('overwrite').save('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_product_details')

# COMMAND ----------

product_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),  # IDENTITY in SQL is handled differently in Spark
    StructField("name", StringType(), nullable=False),
    StructField("quantity_in_stock", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(4, 2), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
])
df=spark.read.format("parquet").schema(product_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_product_details/')
display(df)
df.count()

# COMMAND ----------

from pyspark.sql.types import *
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
df_new = spark.read.format("csv").schema(order_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/order_details/order_details_2024-06-27T12:48:29.6079649Z.csv')
df_new.show()
df_new.write.format('parquet').option('header',True).mode('overwrite').save('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_order_details')

# COMMAND ----------

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
df=spark.read.format("parquet").schema(order_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_order_details/')
display(df)

# COMMAND ----------

from pyspark.sql.types import *
saled_order_items_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(6, 2), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
   ])
df_new = spark.read.format("csv").schema(saled_order_items_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/saled_order_items/saled_order_items_2024-06-27T12:48:51.7875868Z.csv')
df_new.show()
df_new.write.format('parquet').option('header',True).mode('overwrite').save('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_saled_order_items')

# COMMAND ----------

saled_order_items_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(6, 2), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False, metadata={"default": "CURRENT_TIMESTAMP"})
   ])
df=spark.read.format("parquet").schema(saled_order_items_schema).option("header", "true").load('dbfs:/mnt/databrickspratice/incremental_data/raw_data/raw_saled_order_items/')
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

# Initialize Spark session
spark = SparkSession.builder.appName("Max Value Example").getOrCreate()

# Example DataFrame
data = [("2024-01-01", 100), ("2024-01-01", 200), ("2024-01-02", 150), ("2024-01-02", 300)]
columns = ["date", "total_consumption"]
df = spark.createDataFrame(data, columns)

# Group by 'date' and find the max 'total_consumption'
max_values_df = df.groupBy(col('date')).agg(max(col('total_consumption')).alias('max_total_consumption'))

# Collect the result
max_values = max_values_df.collect()

# Print the result
for row in max_values:
    print(f"Date: {row['date']}, Max Total Consumption: {row['max_total_consumption']}")

# If you need the max value for a specific date
max_value = max_values_df.filter(col('date') == '2024-01-01').collect()[0]['max_total_consumption']
print(f"Max Total Consumption on 2024-01-01: {max_value}")

