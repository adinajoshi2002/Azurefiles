# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing Secret Keys
# MAGIC

# COMMAND ----------

dbutils.secrets.help()
dbutils.secrets.list(scope='datalake_scope')
dbutils.secrets.get(scope='datalake_scope',key='DatalakeAccessKey')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Creating link between datalake and databricks

# COMMAND ----------

#Creating link between datalake and databricks
secret_aceess_key=dbutils.secrets.get(scope='datalake_scope',key='DatalakeAccessKey')
storage_account="databrickspratice"
container="refine"
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",secret_aceess_key)



# COMMAND ----------

# MAGIC %md
# MAGIC ######Listing files inside folder in datalake after creating link between datalake and databricks

# COMMAND ----------

#listing all files in unzipped folder
dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/joshi/unzipped/")

# COMMAND ----------

# MAGIC %md 
# MAGIC ######spark.read()
# MAGIC 1. Reading all files from datalake into databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Adding header to raw data in datalake file "SalesOrderHeader.csv"

# COMMAND ----------


# Reading "SalesOrderHeader.csv" from datalake
# Adding  header to file "SalesOrderHeader.csv"
file_path1="abfss://refine@databrickspratice.dfs.core.windows.net/joshi/unzipped/SalesOrderHeader.csv"

df_salesOrder=spark.read.option("sep","\t").csv(file_path1)
display(df_salesOrder)

header=["SalesOrderID","RevisionNumber","OrderDate","DueDate","ShipDate","Status","OnlineOrderFlag","SalesOrderNumber","PurchaseOrderNumber","AccountNumber","CustomerID","ShipToAddressID","OnlineOrdercopen","PurchaseOrderNumberid","SalesOrderNumberID","AccountNumberID","BillToAddressID","ShipMethod","CreditCardApprovalCode","SubTotal","TaxAmt","Freight","TotalDue","Comment","rowguid","ModifiedDate"]

for i in range(len(header)):
   df_salesOrder =df_salesOrder.withColumnRenamed('_c'+str(i),header[i])

display(df_salesOrder)


# COMMAND ----------

#Reading "SalesOrderDetail.csv" file from datalake by using "spark.read.csv()" command

file_path2 ="abfss://refine@databrickspratice.dfs.core.windows.net/joshi/unzipped/SalesOrderDetail.csv"
df_SalesOrderDetail=spark.read.option('header','True').csv(file_path2)
#display(df_product)

# COMMAND ----------

#Reading "Product.csv" file from datalake by using "spark.read.csv()" command

file_path2 ="abfss://refine@databrickspratice.dfs.core.windows.net/joshi/unzipped/Product.csv"
df_product=spark.read.option('header','True').csv(file_path2)
#display(df_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Join()
# MAGIC 1. i need to join "df_product" with "df_SalesOrderDetail" using "productID"
# MAGIC 1. new_df=lefttable_df.join(righttable_df,common column,join type)

# COMMAND ----------

#using join() function by joining "product" and "SalesOrderDetail" data

df_pjsod = df_product.join(df_SalesOrderDetail,df_product.ProductID == df_SalesOrderDetail.ProductID,"inner")
display(df_pjsod)



# COMMAND ----------

# MAGIC %md
# MAGIC ######select() 
# MAGIC 1. selecting requried columns by using select().
# MAGIC 1. select_df=old_df.select(col1,col2,col3,....).
# MAGIC
# MAGIC

# COMMAND ----------

#Select requried columns from dataframe "df_pjsod" 
df_main_product_SalesOrderDetail=df_pjsod.select(df_product.ProductID,(df_pjsod.Name).alias("ProductName"),df_pjsod.ProductNumber,df_pjsod.SalesOrderID,df_pjsod.Color,df_pjsod.Size,df_pjsod.StandardCost,df_pjsod.ListPrice)

display(df_main_product_SalesOrderDetail)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Replace
# MAGIC 1. by using when function in pyspark.sql.functions import when
# MAGIC 1. new_df=old_df.withCloumn(<column_name>,when(<condition>,ValueToRepalce).when(<condition>,ValueToRepalce).otherwise(<place this value>))

# COMMAND ----------

 # Replace size value with requried values

from pyspark.sql.functions import when
df_replace_size = df_main_product_SalesOrderDetail.withColumn("Size",
                                                            when(df_main_product_SalesOrderDetail["Size"]=="S","30")
                                                                 .when(df_main_product_SalesOrderDetail["Size"]=="M","32").
                                                                 when(df_main_product_SalesOrderDetail["Size"]=="L","34").
                                                                 when(df_main_product_SalesOrderDetail["Size"]=="XL","36").
                                                                 when(df_main_product_SalesOrderDetail["Size"]=="XXL","38").otherwise(df_main_product_SalesOrderDetail["Size"]))
display(df_replace_size)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Filter()
# MAGIC 1. by using filter (filtering null values in color column) 
# MAGIC 1. df_filtered=old_df.fliter(old_df[<ColumnName>].isNotNull()) 
# MAGIC 1. but we have null instring formate so we use
# MAGIC 1. df_filtered=old_df.fliter((old_df[<ColumnName1>]!="NULL") & (old_df[<ColumnName2>]!="NULL") )

# COMMAND ----------

# filter NULL vlaues in color and size column

df_filtered_data = df_replace_size.filter((df_replace_size["Color"]!="NULL") & (df_replace_size["Size"]!="NULL"))
display(df_filtered_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Split()
# MAGIC Rename product name in records 

# COMMAND ----------

#Renaming ProductName by removing color and size
from pyspark.sql.functions import split
df_ProdctRename = df_filtered_data.withColumn("ProductName",split(df_filtered_data["ProductName"],'[,-]+')[0])
display(df_ProdctRename)



# COMMAND ----------

# MAGIC %md
# MAGIC ######DataType Casting
# MAGIC

# COMMAND ----------


from pyspark.sql.types import IntegerType

df_ProdctRename.printSchema()

df_datatypecast = df_ProdctRename.withColumn('Size', df_ProdctRename["Size"].cast(IntegerType())) \
                                .withColumn('StandardCost', df_ProdctRename["StandardCost"].cast(IntegerType())) \
                                .withColumn('ListPrice', df_ProdctRename["ListPrice"].cast(IntegerType()))\
                                .withColumn('ProductID', df_ProdctRename["ProductID"].cast(IntegerType()))\
                                .withColumn('SalesOrderID', df_ProdctRename["SalesOrderID"].cast(IntegerType()))

df_datatypecast.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Discount cost of product sold by diff between "ListPrice" and "StandaradCost"

# COMMAND ----------

# finding difference between "ListPrice" and "StandaradCost"

from pyspark.sql.functions import col,round
df_final_data=df_datatypecast.withColumn("DiffPrice",round(col("ListPrice")-col("StandardCost"),2))

# COMMAND ----------

# MAGIC %md 
# MAGIC ######Creating view tables of "df_final_data","df_salesOrder","df_product"

# COMMAND ----------

#creating viewtables 
df_salesOrder.createOrReplaceTempView("df_salesOrder")
df_final_data.createOrReplaceTempView("df_final_data")
df_product.createOrReplaceTempView("df_product")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Created Managed Tables "manage_ProductSalesOrderDetails","manage_SalesOrderDetails","manage_Product"

# COMMAND ----------

df_final_data.write.saveAsTable("manage_ProductSalesOrderDetails")
df_salesOrder.write.saveAsTable("manage_SalesOrderDetails")
df_product.write.saveAsTable("manage_ProductDetails")


# COMMAND ----------

# MAGIC %md
# MAGIC #####Describe table schema

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table manage_SalesOrderDetails;
# MAGIC show create table manage_ProductSalesOrderDetails;
# MAGIC show create table manage_ProductDetails;

# COMMAND ----------

# MAGIC %md
# MAGIC ######Creating external table "productdetails" load data into datalake into processed container with file name "productdetails"

# COMMAND ----------

spark.sql(''' CREATE TABLE if NOT EXISTS  ProductDetails (   ProductID STRING,   Name STRING,   ProductNumber STRING,   Color STRING,   StandardCost STRING,   ListPrice STRING,   Size STRING,   Weight STRING,   ProductCategoryID STRING,   ProductModelID STRING,   SellStartDate STRING,   SellEndDate STRING,   DiscontinuedDate STRING,   ThumbNailPhoto STRING,   ThumbnailPhotoFileName STRING,   rowguid STRING,   ModifiedDate STRING) 
USING DELTA LOCATION  'abfss://processed@databrickspratice.dfs.core.windows.net/productdetails' ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Inserting records into external table "productdetails" from "df_product" 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ProductDetails
# MAGIC select * from df_product;
# MAGIC
# MAGIC /* 
# MAGIC %python
# MAGIC df_product.write.option("path","abfss://processed@databrickspratice.dfs.core.windows.net/productdetails").saveAsTable("ProductDetails")
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ######Creating external table "FinalProductSalesOrderDetails" load data into datalake into processed container with file name "FinalProductSalesOrderDetails"

# COMMAND ----------

spark.sql('''CREATE TABLE if NOT EXISTS FinalProductSalesOrderDetails (   ProductID STRING,   ProductName STRING,   ProductNumber STRING,   SalesOrderID STRING,   Color STRING,   Size INT,   StandardCost INT,   ListPrice INT,   DiffPrice INT) USING delta LOCATION  'abfss://processed@databrickspratice.dfs.core.windows.net/FinalProductSalesOrderDetails' ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Inserting records into external table "FinalProductSalesOrderDetails" from "df_final_data" 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into FinalProductSalesOrderDetails
# MAGIC select * from df_final_data;
# MAGIC
# MAGIC
# MAGIC /* 
# MAGIC %python
# MAGIC df_final_data.write.option("path","abfss://processed@databrickspratice.dfs.core.windows.net/FinalProductSalesOrderDetails").saveAsTable("FinalProductSalesOrderDetails")
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ######Creating external table "SalesOrderDetails" load data into datalake into processed container with file name "SalesOrderDetails"

# COMMAND ----------

spark.sql('''CREATE TABLE if NOT EXISTS SalesOrderDetails (SalesOrderID STRING,   RevisionNumber STRING,   OrderDate STRING,   DueDate STRING,   ShipDate STRING,   Status STRING,   OnlineOrderFlag STRING,   SalesOrderNumber STRING,   PurchaseOrderNumber STRING,   AccountNumber STRING,   CustomerID STRING,   ShipToAddressID STRING,   OnlineOrdercopen STRING,   PurchaseOrderNumberid STRING,   SalesOrderNumberID STRING,   AccountNumberID STRING,   BillToAddressID STRING,   ShipMethod STRING,CreditCardApprovalCode STRING,SubTotal STRING,TaxAmt STRING,Freight STRING,TotalDue STRING,Comment STRING,rowguid STRING,ModifiedDate STRING) USING delta LOCATION  'abfss://processed@databrickspratice.dfs.core.windows.net/SalesOrderDetails' ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Inserting records into external table " SalesOrderDetails" from "df_salesOrder" 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into SalesOrderDetails
# MAGIC select * from df_salesOrder;
# MAGIC
# MAGIC
# MAGIC /* 
# MAGIC %python
# MAGIC df_salesOrder.write.option("path","abfss://processed@databrickspratice.dfs.core.windows.net/SalesOrderDetails").saveAsTable("SalesOrderDetails")
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ######visualization on transformed and join dataframe "df_final_data"

# COMMAND ----------

# for visualization after doing transformations and joining on product and salesdetails
display(df_final_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table default.SalesOrderDetails;
# MAGIC --drop table default.FinalProductSalesOrderDetails;
# MAGIC --drop table default.ProductDetails;
# MAGIC --drop table default.manage_SalesOrderDetails;
# MAGIC --drop table default.manage_FinalProductSalesOrderDetails;
# MAGIC --drop table default.manage_ProductDetails;
# MAGIC
# MAGIC
