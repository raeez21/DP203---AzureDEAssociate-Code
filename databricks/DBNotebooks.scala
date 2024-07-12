// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
// val file_location = "/FileStore/parquet/log.parquet"
// val file_type = "parquet"
val file_location = "/FileStore/csv/Log.csv"
val file_type = "csv"
val dataSchema = StructType(Array(
    StructField("Correlationid", StringType, true),
    StructField("Operationname", StringType, true),
    StructField("Status", StringType, true),
    StructField("Eventcategory", StringType, true),
    StructField("Level", StringType, true),
    StructField("Time", TimestampType, true),
    StructField("Subscription", StringType, true),
    StructField("Eventinitiatedby", StringType, true),
    StructField("Resourcetype", StringType, true),
    StructField("Resourcegroup", StringType, true),
    StructField("Resource", StringType, true)
))

val df = spark.read.format(file_type).
options(Map("header"->"true")).
schema(dataSchema).
load(file_location)


// COMMAND ----------

display(df)

// COMMAND ----------

// Group by and filter operationname whose count >100
display(df.groupBy(col("Operationname")).
count().alias("Count").filter(col("Count")>100))

// COMMAND ----------

// some date functions
display(df.select(year(col("time")), month(col("time")),dayofyear(col("time")),col("time")))

// COMMAND ----------

display(df.select(to_date(col("time"), "dd-mm-yyyy").alias("Date")))

// COMMAND ----------


// FIltering on Null values
val dfNull = df.filter(col("Resourcegroup").isNull)
dfNull.count()

// COMMAND ----------

val dfNotNull = df.filter(col("Resourcegroup").isNotNull)
dfNotNull.count()

// COMMAND ----------

//Save the df to a table
df.write.saveAsTable("logdata")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM logdata

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE EXTENDED logdata

// COMMAND ----------

// Task 2
// Reading  data from ADLS
// We are accessing via account keys

val file_location = "abfss://csv@adlsraeez.dfs.core.windows.net/Log.csv"
val file_type = "csv"
//Give the account key
spark.conf.set(
  "fs.azure.account.key.adlsraeez.dfs.core.windows.net",
  "JYgP7N+JaMow0Kt1GYi7qkKNZ5MgdmdIfaSVQ2rP+KuzsA62+AojZYtmDJEki+mO6lWnkaoCbM2V+AStWVhLDQ=="
)
val dataSchema = StructType(Array(
    StructField("Correlationid", StringType, true),
    StructField("Operationname", StringType, true),
    StructField("Status", StringType, true),
    StructField("Eventcategory", StringType, true),
    StructField("Level", StringType, true),
    StructField("Time", TimestampType, true),
    StructField("Subscription", StringType, true),
    StructField("Eventinitiatedby", StringType, true),
    StructField("Resourcetype", StringType, true),
    StructField("Resourcegroup", StringType, true),
    StructField("Resource", StringType, true)
))

val df = spark.read.format(file_type).
options(Map("header"->"true")).
schema(dataSchema).
load(file_location)

// COMMAND ----------

display(df)

// COMMAND ----------

// Process JSON data
val dfJson = spark.read.format("json").load("/FileStore/json/customer_arr.json")
display(dfJson )

// COMMAND ----------

val newJson  = dfJson.select(col("customerid"),col("customername"), explode(col("courses")))
display(newJson)

// COMMAND ----------

// Objects within objects
val dfObjJson = spark.read.format("json").load("/FileStore/json/customer_obj.json")
display(dfObjJson )

// COMMAND ----------

val newJson  = dfObjJson.select(col("customerid"),col("customername"), explode(col("courses")), col("details.city"), col("details.mobile"))
display(newJson)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Taks 3
// MAGIC -- Use external source and use COPY INTO command to store it into delta table within Datbricks
// MAGIC CREATE DATABASE appdb

// COMMAND ----------

// MAGIC %sql
// MAGIC USE appdb

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE logdata
// MAGIC --  Here we havent given a schema while creating table unlike other SQL stores
// MAGIC -- So here schema is inferred when data is uploaded to tble
// MAGIC -- In delta table, data is stored in files and the structure is stored on metastore
// MAGIC

// COMMAND ----------

//set the account key so that this notebook can access the data in ADLS
spark.conf.set(
  "fs.azure.account.key.adlsraeez.dfs.core.windows.net",
  "JYgP7N+JaMow0Kt1GYi7qkKNZ5MgdmdIfaSVQ2rP+KuzsA62+AojZYtmDJEki+mO6lWnkaoCbM2V+AStWVhLDQ=="
)


// COMMAND ----------

// MAGIC %sql
// MAGIC COPY INTO logdata
// MAGIC FROM 'abfss://parquet-adf@adlsraeez.dfs.core.windows.net/Log.parquet'
// MAGIC FILEFORMAT = PARQUET
// MAGIC COPY_OPTIONS('mergeSchema' = 'true');
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM logdata

// COMMAND ----------

// MAGIC %sql
// MAGIC --Task 4
// MAGIC --Streaming data using databricks
// MAGIC -- create the target table
// MAGIC USE appdb;
// MAGIC CREATE TABLE DimCustomer(
// MAGIC   CustomerID STRING,
// MAGIC   CompanyName STRING,
// MAGIC   SalesPerson STRING
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM dimCustomer

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.adlsraeez.dfs.core.windows.net",
  "JYgP7N+JaMow0Kt1GYi7qkKNZ5MgdmdIfaSVQ2rP+KuzsA62+AojZYtmDJEki+mO6lWnkaoCbM2V+AStWVhLDQ=="
)
val path = "abfss://csv@adlsraeez.dfs.core.windows.net/dimCustomers/"
val checkpointPath = "abfss://checkpoint@adlsraeez.dfs.core.windows.net/"
val schemaLocation = "abfss://schema@adlsraeez.dfs.core.windows.net/"

// COMMAND ----------

//use Spark session's readStream() to read incoming stream of data
//here we using Autoloader to automatically detect the schema of loaded data--->this supports schema evolution
// for schema evolution, it needs to have some location that keeps track of exisitng schema, that is why we need schemaLocation
val dfDimCustomer = (spark.readStream.format("cloudfiles")
                      .option("cloudFiles.schemaLocation",schemaLocation)
                      .option("cloudFiles.format","csv")
                      .load(path))

//after data loaded to dataframe, save this into table using writeStream()
// checkpoint location is to keep track of blobs which was already streamed. This ensures data is not read twice
dfDimCustomer.writeStream.format("delta")
.option("checkpointLocation",checkpointPath)
.option("mergeSchema","true")
.table("DimCustomer")

// COMMAND ----------

// MAGIC %sql
// MAGIC --  Task 4
// MAGIC -- Removing duplicates
// MAGIC SELECT CustomerID, Count(CustomerID)
// MAGIC From dimCustomer
// MAGIC Group by CustomerID
// MAGIC HaVING COUNT(CustomerID) > 1

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dimCustomer 

// COMMAND ----------

//use Spark session's readStream() to read incoming stream of data
//here we using Autoloader to automatically detect the schema of loaded data--->this supports schema evolution
// for schema evolution, it needs to have some location that keeps track of exisitng schema, that is why we need schemaLocation


val dfDimCustomer = (spark.readStream.format("cloudfiles")
                      .option("cloudFiles.schemaLocation",schemaLocation)
                      .option("cloudFiles.format","csv")
                      .load(path))

// Now delete the duplicates
val finalDimCustomer = dfDimCustomer.dropDuplicates("CustomerID")
//after dropping duplicate data, save this into table using writeStream()
// Go to the checkpoint container and delete the files in in, to let Databricks read the same file again
// checkpoint location is to keep track of blobs which was already streamed. This ensures data is not read twice
finalDimCustomer.writeStream.format("delta")
.option("checkpointLocation",checkpointPath)
.option("mergeSchema","true")
.table("DimCustomer")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM DimCustomer

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT CustomerID, Count(CustomerID)
// MAGIC From dimCustomer
// MAGIC Group by CustomerID
// MAGIC HaVING COUNT(CustomerID) > 1

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Task 5
// MAGIC -- Speciying shcema
// MAGIC DROP TABLE DimCustomer;
// MAGIC CREATE TABLE DimCustomer(
// MAGIC   CustomerID INT, -- changed to INT
// MAGIC   CompanyName STRING,
// MAGIC   SalesPerson STRING
// MAGIC )

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.adlsraeez.dfs.core.windows.net",
  "JYgP7N+JaMow0Kt1GYi7qkKNZ5MgdmdIfaSVQ2rP+KuzsA62+AojZYtmDJEki+mO6lWnkaoCbM2V+AStWVhLDQ=="
)
val path = "abfss://csv@adlsraeez.dfs.core.windows.net/dimCustomers/"
val checkpointPath = "abfss://checkpoint@adlsraeez.dfs.core.windows.net/"
val schemaLocation = "abfss://schema@adlsraeez.dfs.core.windows.net/"

// COMMAND ----------


//  define the schenma construct
import org.apache.spark.sql.types._
val dataSchema = StructType(Array(
                  StructField("CustomerID",IntegerType, true),
                  StructField("CompanyName",StringType, true),
                  StructField("SalesPerson",StringType, true)))

// COMMAND ----------

val dfDimCustomer = (spark.readStream.format("cloudfiles")
                      .schema(dataSchema) //mention the schema
                      // since schema specified, no need to give SchemaLocation
                      .option("header","true")
                      .option("cloudFiles.format","csv")
                      .load(path))
val finalDimCustomer = dfDimCustomer.dropDuplicates("CustomerID")
finalDimCustomer.writeStream.format("delta")
.option("checkpointLocation",checkpointPath)
.option("mergeSchema","true")
.table("DimCustomer")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM dimCustomer;

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Task 6
// MAGIC -- Versioning of tables
// MAGIC DESCRIBE HISTORY dimCustomer

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dimCustomer
// MAGIC VERSION AS OF 0

// COMMAND ----------

// Task 7
// Read data from Synapse table and load into df 

//Set the key for ADLS for the staging  location
spark.conf.set(
  "fs.azure.account.key.adlsraeez.dfs.core.windows.net",
  "JYgP7N+JaMow0Kt1GYi7qkKNZ5MgdmdIfaSVQ2rP+KuzsA62+AojZYtmDJEki+mO6lWnkaoCbM2V+AStWVhLDQ=="
)


val df = spark.read
        .format("com.databricks.spark.sqldw")
        .option("url","jdbc:sqlserver://dpraeez.sql.azuresynapse.net:1433;database=datapool")
        .option("user","sqladmin")
        .option("password","enteryourpasshere")
        .option("tempDir","abfss://staging@adlsraeez.dfs.core.windows.net/databricks")
        .option("forwardSparkAzureStorageCredentials","true")
        .option("dbTable","BlobDiagnostics")
        .load()
display(df)

// COMMAND ----------

// Similarly here write data onto table in Synapse
//  Here we stream data residing in ADLS to Databricks and then write to Synapse table
import org.apache.spark.sql.types._

val path = "abfss://csv@adlsraeez.dfs.core.windows.net/dimCustomers/"
val checkpointPath = "abfss://checkpoint@adlsraeez.dfs.core.windows.net/"
val dataSchema = StructType(Array(
                  StructField("CustomerID",IntegerType, true),
                  StructField("CompanyName",StringType, true),
                  StructField("SalesPerson",StringType, true)))

// COMMAND ----------

val dfDimCustomer = (spark.readStream.format("cloudfiles")
                      .schema(dataSchema) //mention the schema
                      // since schema specified, no need to give SchemaLocation
                      .option("header","true")
                      .option("cloudFiles.format","csv")
                      .load(path))
dfDimCustomer.writeStream
              .format("com.databricks.spark.sqldw")
              .option("url","jdbc:sqlserver://dpraeez.sql.azuresynapse.net:1433;database=datapool")
              .option("user","sqladmin")
              .option("password","Raeez@212121")
              .option("tempDir","abfss://staging@adlsraeez.dfs.core.windows.net/databricks")
              .option("forwardSparkAzureStorageCredentials","true")
              .option("dbTable","DimCustomerNew")
              .option("checkpointLocation",checkpointPath)
              .start()

// COMMAND ----------


