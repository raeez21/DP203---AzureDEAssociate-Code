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
// MAGIC CREATE TABLE DimCustomer(
// MAGIC   CustomerID STRING,
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
// MAGIC SELECT * FROM DimCustomer

// COMMAND ----------


