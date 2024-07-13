// Databricks notebook source
// MAGIC %sql
// MAGIC --Task 8 and 9
// MAGIC use appdb

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table logdata

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
spark.conf.set(
  "fs.azure.account.key.adlsraeez.dfs.core.windows.net",
  "JYgP7N+JaMow0Kt1GYi7qkKNZ5MgdmdIfaSVQ2rP+KuzsA62+AojZYtmDJEki+mO6lWnkaoCbM2V+AStWVhLDQ=="
)

// COMMAND ----------

val file_location = "abfss://csv@adlsraeez.dfs.core.windows.net/Log.csv"
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
df.write.saveAsTable("appdb.logdata")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM appdb.logdata

// COMMAND ----------


