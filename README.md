# DP203 AzureDEAssociate-Code

This repo contains main code utilised while preparing for DP 203 course Azure Data Engineer Associate Certification

# 1. Synapse
 One of the initial task was to read files contained in an ADLS container from Synapse with SQL queries using Serverless SQL pools. This was done by creating External tables (Synapse manages the metadata but the underlying data is still stored in ADLS container).
 ## Serverless SQL pools
 1. [ExternalTableFromCsvAdls.sql](ExternalTableFromCsvAdls.sql)

    Contains the code to create an external table which points to a csv file in an ADLS container. The access here was given using IAM role (Storage Blob Reader) in the ADLS container UI to the user.

    Here we created a new database, a new data source, a new file format and finally the external table

2. [ExternalTableFromParquetAdls.sql](ExternalTableFromParquetAdls.sql)

    This contains SQL code to create external table pointing to a parquet file in ADLS. The access was given using Credentials (SAS token).

    Here we created a master encryption key, then the scoped credential, data source (which uses this creds and sas token), file format (parquet) and finally the external table

3. [ExternalTableMultipleParquet.sql](ExternalTableMultipleParquet.sql)
   
   This is same as previous one except it reads all parqeut file in a container to the table. This is done by use of wildcard character '*' in LOCATION field of CREATE TABLE STATEMENT. Rest everything remains same as of 2.

5. [OPENROWSETJSON.sql](OPENROWSETJSON.sql)

   This file contains the usage of OPENROWSET function which allows to access files in Azure Storage. This function reads contents of a remote data source (for ex: a file) amd returns content as set of rows. Access in this file is given using IAM role. Make note of parameters used like FIELDTERMINATOR, FIELDQUOTE etc.
   
## Dedicated SQL pools

 To host a SQL DW we need to make use of dedicated sql pools to persist the data. Dedicated SQL pools can also be used to create external tables, and first few files below shows that.

 1. [SqlPoolExternalTableParquet.sql](SqlPoolExternalTableParquet.sql)

    This program creates an external table from a Parquet file in ADLS using dedicated SQL pool. So create a dedidcated SQL pool and then connect to it using 'Connect to' dropdown in SQL editor of Synapse.
    Since this from Parquet file, we can create Native External tables. So the codes remain almost the same.

 2. [SqlPoolExternalTableCsv.sql](SqlPoolExternalTableCsv.sql)

    This script creates an external table from a CSV file in ADLS using dedicated SQL pool. Since it is CSV, we need to create Hadoop External table. So there are few changes like:

    - using abfss protocol while specifiying LOCATION while defining EXTERNAL DATA SOURCE
    - 'TYPE' is given as 'HADOOP'
 ## Loading Data to dedicated SQL pools

 We can create normal persisted table on dedicated SQL pool and load data using various ways.
 1. [LoadDataToNormalTable.sql](LoadDataToNormalTable.sql)

    This file shows various ways:

      - Create a new normal table using existent external table using CTAS stmt. The underlying technology used here is Polybase
      - Creating a normal table using COPY INTO command which loads data from CSV file in ADLS
      - Creating a normal table using COPY INTO command which loads data from Parquet file in ADLS
      - Load data using 'Pipeline' which loads data from file in ADLS container
      - use 'Pipelines' to transfer data from table in a relational DB(Azure SQL DB) to table in dedicated SQL Pool (Data Warehouse).
   
 2. [ETL_Scripts.sql](ETL_Scripts.sql)

    Our Azure SQL DB acts as a OLTP database from which we pull data into a DW. The data in DW is in dimensional model (facts and dimensions).
    This script contains code to create tables which will store fact and dim tables. The source for these tables are in Azure SQL DB tables and we pull the data using Pipelines (Integrate tab on Synapse studio)

    After Creating these tables in Synapse, now go create the Pipeline in Integration table. While specifying source Instead of 'Tables' choose 'Query' and copy SQL stmts found in the last section of this script. We have 1 fct and 2 dim tables...so repeat this activity 3 times one for each table


3. [OptmisedETLScripts.sql](OptmisedETLScripts.sql)
   
    This script contains some optimised code icnluding hash distributed fact tables and replicated dimension tables. It also depicts the use of Surrogate Keys (SKs)

4. [TablePartitions.sql](TablePartitions.sql)

   THis script contains code for understanding table partitions. We redefine previously created table with partitioned on a date column. Partition Switching is also performed in this script

## Synapse Architecture

 ![synapse architecture](Synapse%20architecture.png)
 
 In Synapse, compute and storage are separate so that each can be scaled separately. The user data is stored in Azure Storage Account. 

 All the queries are targeted towards control node. Then control node distributes query for parallel processing across compute nodes.

 Refer about distribution and sharding in Synapse.


   TODO: Do ADF, [ADF_scripts.sql], [MappingDataFlow.sql]

# 2. Azure Data Factory (ADF)

ADF is a cloud based ETL tool and integration service. It is used to for orchestrating data movement and transforming data at scale.  The underlying compute infrastructre (Integration runtime) is managed for you.

1. [ADF_scripts.sql](ADF_scripts.sql)

    This file has script and steps to create simple pipelines.
     - Task 1 = create a simple pipeline to copy data from 'Log.csv' (in ADLS) into a table in Synapse (Copy_adls_to_synapse Pipeline)
     - Task 2 = Copy data from csv contianer to Parquet container (both in ADLS) (Copy_To_Parquet Pipeline)
     - Task 3 = Modify task 2 to copy the output parquet file to a table in Synapse (Copy_To_Parquet Pipeline)
     - Task 4 = Use a query in copy data tool to transfer data. we transfer data present in Azure SQL db to a table in Synapse (CopyUsingQuery_sqlDB_To_Synapse Pipeline)
     - Task 5 = Adding additional columns (Copy_To_Parquet Pipeline)
     - Task 6 = Copy data using Copy command (Copy_To_Parquet Pipeline)
     - task 7 = Copy data usinng PolyBase

2. [MappingDataFlow.sql](MappingDataFlow.sql)

     This section has details code for learning Mapping Data flows in ADF. Mapping Data flows are usd when we have complex transformations to perform. The data flows run on a Spark CLsuter instead of Azure Integrtaion Runtime (which was used by Copy Data tool).
    This file has commands for:
     - Develop simple data flows to populate Fact and Dimesnion tables
     - Adding derived column with dynamic values in fact_sales table
     - Add a Surrogate Key to the Dim table using Mapping data flow (This was also done using Synapse, but ADF is better due to the proper sequence of the SK)
     - Cache Sink and Lookup (VERY IMP) to continue the sequence of CustomerSK across different loads.
     - Handling duplicate rows using Exists activity
     - Filter rows while transfering data from source to destination using filter' row modifier option
     - Generate JSON data from Parqeut based files, i.e take Parquet file from a container and store it as JSON in another container
     - Extend the above where we take the JSON from container to table in Synapse
     - process JSON data with nested structure (JSON arrays) using 'Flatten' formatter
     - Processing nested JSON objects
     - Conditional Split (We only want the records belonging to particular Resourcegroup (for eg: 'app-grp') to be pushed to the sink table)
     - Schema Drift (VERY IMP)
     - Get Metadata activity and ForEach
     - Using and running Stored Procedure through ADF
     - Lookup activity instead of Stored Procedure to get the output from the SP.
     - Running a pipeline (Copy_To_Parquet) based on Storage event trigger
     - Other triggers are: schedule trigger and tumbling window trigger
2. [SelfHostedIR.sql](SelfHostedIR.sql)

     FOr all the activities till now we were using Azure Integrated Runtime which provides compute infrastructure for our tasks (were bpth source and sinks were Azure services). This is not the case always, our data might be hosted on an on prem server. To use this server machine you need to register the server with ADF and to register you need to install Self Hosted IR.

     In this program, We take log files situated in a separate VM. We created a VM instance, installed the Self hosted IR and load log file from VM into a container using Pipeline Copy Activity.
   (VERY IMP)
# 3. Event Hub and Streaming Analytics
Event Hubs is a multi-protocol event streaming engine that natively supports Advanced Message Queuing Protocol (AMQP), Apache Kafka, and HTTPS protocols. It can receive and stream millions of events per second. Comapnies use Event Hubs to ingest millions of events per second from connected devices and applications. Azure Stream Analytics is a fully managed stream processing engine that is designed to analyze and process large volumes of streaming data with sub-millisecond latencies. 

[EventhubsStreamAnalytics.sql](EventhubsStreamAnalytics.sql) has code and info about using Event Hubs and Stream Analytics: 

We created a namespace and hub in the Event Hub service and also Stream Analytics Job. Then we have a .Net pgm [EventHub/AzureEventHub-Send/AzureEventHub-Send/Program.cs](EventHub/AzureEventHub-Send/AzureEventHub-Send/Program.cs) which produces some Event data (list of order info) and sends to the Event Hub.
   - Task 1: These events present in Event Hub are then taken by Stream Analytics which processes the events and deliver into destination (Synapse).
   - Task 2: Diagnostic logs from ADLS is streamed to Event Hub and from there processed in  Stream Analytics
   - Task 3: Here we take diagnostic setting for another service called Azure Web App service.
   - Task 4: Window based functions in Stream Analytics (Tumbling window)
   - Task 5: using reference data
   - Task 6: NSG flow logs (complex nested log file)
   - Task 7: Using ADF for batch processing logs
