// Databricks notebook source
// import com.microsoft.azure.eventhubs.ConnectionStringBuilder
import org.apache.spark.eventhubs._

// Got eventhub instance -> Shared access policy --> Choose the policy -> Copy the connection string
val eventhubConnectionString = ConnectionStringBuilder("Endpoint=sb://datanamespacedp203.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=R76Q42CbfSLLvUEBYSXFjNreAfVrySlRn+AEhMVj9II=;EntityPath=blobhub")
                                // .setEventHubName("appeventhub")
                                .build()
val eventHubConfiguration = EventHubsConf(eventhubConnectionString)

// COMMAND ----------

// Load the data from event hub

//When databricks reads data from Event Hub, the payload is put in binary format and is sent as body of data block
var apphubDF = spark.readStream.format("eventhubs").options(eventHubConfiguration.toMap).load()
val recordsDF = apphubDF.select(get_json_object(($"body").cast("string"),"$.records").alias("records"))
display(recordsDF)
