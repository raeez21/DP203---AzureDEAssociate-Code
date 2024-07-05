-- This file contains code and instructions for the Event Hub and Stream Analytics section

-- Task 1
-- We have a .Net program which produces some Event data (list of order info) and sends to the Event Hub.
-- These events present in Event Hub are then taken by Stream Analytics which processes the events and deliver into destination

-- Orders Table, to store events coming from Event Hub via Stream Analytics
-- .Net pgm that creates order events --> Event Hub ---> Stream Analytics --> Below table in Synapse
CREATE TABLE Orders(
    OrderID VARCHAR(10),
    Quantity INT,
    UnitPrice decimal(5,2),
    DiscountCategory VARCHAR(10)
)
-- Create Event Hub "appeventhub"
-- Go to Shared Access Policy of Event Hub, create a policy and copy the Connection String
-- Paste this Connection string in .Net pgm (AzureEventHub-Send/Program.cs)

-- Now create a Stream Analytics Job (appStreamAnalytics)
    -- Set Input as appeventhub
    -- Set Output as Orders table in Synapse (OrdersTableSynapse)
    -- Set Query as below query
SELECT
    OrderID, Quantity, UnitPrice, DiscountCategory --DOnt do select * here, bcoz even hub has other columns like time loaded, partition id etc.
INTO
    [OrdersTableSynapse]
FROM
    [appeventhub]
where OrderID is not null -- because we had some test events in our hub which is of very different schemaa

-- TIll now our infra is set, now test the system by producing sample data
-- In Visual Studio Code Terminal do below command to run the program which generates orders data to Event Hub
dotnet run --project .\AzureEventHub-Send.csproj
-- Query in Stream analytics


--DELETE FROM Orders
-- Run the Stream analytics job, wait for sometime and validate by selecting contents from the table:
SELECT * FROM ORDERS
order by orderID desc;

SELECT COUNT(*) FROM Orders;



-- Task 2
-- Diagnostic logs from ADLS is streamed to Event Hub and from there processed in  Stream Analytics
-- Create a Diagnostic Setting (LogSettingToStorageAccount) inside Storage Account adlsraeez
-- First point stream this to Diagnostic Setting to another container datastoredp203raeez
-- GO to this container and download the log file to understand its structure to write the query.
-- Now point the Diagnostic Setting(LogSettingToStorageAccount) to stream to Event Hub (blobhub) as well.

-- Create the table to store data in Synapse
CREATE TABLE BlobDiagnostics(
    Time DATETIME,
    Category VARCHAR(30),
    OperationName VARCHAR(300),
    StatusCode int,
    CallerIpAddress VARCHAR(200),
    IdentityType varchar(5000)
)

-- Now the data in Stream Analytics is in below form
-- We need to write a query to correctly capture the fields.
-- [
--   {
--     "records": [
--       {
--         "time": "2024-07-03T16:59:50.6999743Z",
--         "resourceId": "/subscriptions/64f0647d-fd2d-4954-8848-2c51f0da28b3/resourceGroups/dp203_course/providers/Microsoft.Storage/storageAccounts/adlsraeez/blobServices/default",
--         "category": "StorageRead",
--         "operationName": "GetFileProperties",
--         "operationVersion": "2018-11-09",
--         "schemaVersion": "1.0",
--         "statusCode": 200,
--         "statusText": "Success",
--         "durationMs": 8,
--         "callerIpAddress": "10.0.0.135",
--         "correlationId": "b2de677e-a01f-001f-5b6a-cd3364000000",
--         "identity": {
--           "type": "AccountKey",
--           "tokenHash": "key1(4E2BBF32E161B7896CB96CC1EBBD7B23EC4EEC66C877ABFF30375C323B1247A6)"
--         },
--         "location": "uksouth",
--         "properties": {
--           "accountName": "adlsraeez",
--           "userAgentHeader": "AzureDataFactoryCopy FxVersion/4.8.4718.0 OSName/Windows OSVersion/6.2.9200.0 Microsoft.Azure.Storage.Data.AzureDfsClient/1.1.8922.29",
--           "serviceType": "blob",
--           "objectKey": "/adlsraeez/parquet-adf/Log.parquet",
--           "lastModifiedTime": "2024/07/03 16:59:35.0118188",
--           "metricResponseType": "Success",
--           "serverLatencyMs": 8,
--           "requestHeaderSize": 386,
--           "responseHeaderSize": 417,
--           "tlsVersion": "TLS 1.3",
--           "accessTier": "None"
--         },
--         "uri": "https://adlsraeez.dfs.core.windows.net/parquet-adf/Log.parquet",
--         "protocol": "HTTPS",
--         "resourceType": "Microsoft.Storage/storageAccounts/blobServices"
--       }
--     ],
--     "EventProcessedUtcTime": "2024-07-03T17:13:46.3817983Z",
--     "PartitionId": 3,
--     "EventEnqueuedUtcTime": "2024-07-03T17:01:01.3380000Z"
--   },
--   {
--     "records": [
--       {
--         "time": "2024-07-03T16:59:30.3752782Z",
--         "resourceId": "/subscriptions/64f0647d-fd2d-4954-8848-2c51f0da28b3/resourceGroups/dp203_course/providers/Microsoft.Storage/storageAccounts/adlsraeez/blobServices/default",
--         "category": "StorageRead",
--         "operationName": "GetFilesystemProperties",
--         "operationVersion": "2018-06-17",
--         "schemaVersion": "1.0",
--         "statusCode": 200,
--         "statusText": "Success",
--         "durationMs": 11,
--         "callerIpAddress": "10.0.1.54",
--         "correlationId": "8475deeb-b01f-005e-356a-cd6b80000000",
--         "identity": {
--           "type": "AccountKey",
--           "tokenHash": "key1(4E2BBF32E161B7896CB96CC1EBBD7B23EC4EEC66C877ABFF30375C323B1247A6)"
--         },
-- Based on the above structure the query to be given in Stream Analytics is:
-- Note the use of CROSS APPLY GetArrayElements() function to read nested arrays
SELECT
    Records.ArrayValue.time as Time,
    Records.ArrayValue.category as Category,
    Records.ArrayValue.operationName as OperationName,
    Records.ArrayValue.statusCode as StatusCode,
    Records.ArrayValue.callerIpAddress as CallerIpAddress,
    Records.ArrayValue.[identity].type as IdentityType
INTO
    [BlobDiagnosticsTableSynapse]
FROM
    blobhub b
    CROSS APPLY GetArrayElements(b.records) As Records;
-- In stream analytics
    -- Input is blobhub
    -- query is above query
    -- Output the corresp table created above in Synapse (BlobDiagnosticsTableSynapse)

-- Run the Stream analytics job, wait for sometime and validate by selecting contents from the table:
SELECT * FROM BlobDiagnostics;
SELECT COUNT(*) FROM BlobDiagnostics;



-- Task 3
-- Here take diagnostic setting for another service called Azure Web App service.
-- Web App Service is a Platform As a Service that lets you host a web app on Azure platfrom

-- Create a web app service resource (webappRaeez)
-- For this, create a diagnostic setting (LogSettingToStorageAccount) to stream metrics data to a ADLS container (datastoredp203raeez)
-- Go to this container, download file and understand its structure

-- The diagnostic logs of Web App service is streamed first to a container, from there to Stream Analytics and finally onto table
-- Now go to Stream Analytics job (WebAppStreamJob):
    -- Input is ADLS location (InsightsMetricsADLS) which uses a common path pattern --- VIMP
    -- Output is the output table in Synapse (WebMetricsTableSynapse )
    -- Query used is given below

-- For input, use common path pattern to read all JSON files contained in different subdirectories
-- USe tthis as dynamic common path pattern
resourceId=/SUBSCRIPTIONS/64F0647D-FD2D-4954-8848-2C51F0DA28B3/RESOURCEGROUPS/APP-GRP/PROVIDERS/MICROSOFT.WEB/SITES/WEBAPPRAEEZ/y={datetime:yyyy}/m={datetime:MM}/d={datetime:dd}/h={datetime:HH}/m={datetime:mm}

-- Create the output table in Synapse
CREATE TABLE WebMetrics(
    Minimum decimal,
    Maximum DECIMAL,
    Average DECIMAL,
    Metrictime DATETIME,
    MetricName VARCHAR(200)
)


-- Query in stream analytics
SELECT
    minimum as Minimum,
    maximum as Maximum,
    average as Average,
    time as Metrictime,
    metricName as MetricName
INTO
    [WebMetricsTableSynapse]
FROM
    InsightsMetricsADLS

--Run the Stream analytics job, wait for sometime and validate by selecting contents from the table:
SELECT * FROM WebMetrics

--TASK 4
-- Window based functions in Stream Analytics
-- Read more at: https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-window-functions
SELECT MetricName, MetricTime, COUNT(*) as 'Count of Record found'
FROM WebMetrics
GROUP BY MetricName, MetricTime
ORDER BY MetricName, MetricTime
-- The above query shows that data is coming every minute
-- maybe we want metrics over a particular time window
-- There are different window functions

-- 1. Tumbling Window

-- Create a new table
-- Table to collect windowed summary data
CREATE TABLE WebMetricsSummary(
    MetricName VARCHAR(200),
    MetricAverage decimal,
    WindowTime DATETIME
)

-- the query used in Stream analytics
SELECT
    AVG(average) as MetricAverage,
    MAX(CAST(time as datetime)) as WindowTime,
    metricName as MetricName
INTO
    [WebMetricsSummaryTableSynapse]
FROM
    InsightsMetricsADLS
GROUP BY 
    metricName, TumblingWindow(minute, 10) --window of every 10 mins

-- Start the job and validate the output
SELECT * FROM WebMetricsSummary
SELECT COUNT(*) FROm WebMetricsSummary
SELECT DISTINCT(WindowTime) from WebMetricsSummary
SELECT TOP 60  *  FROM WebMetrics

--Common query patterns
-- https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-stream-analytics-query-patterns


-- Task 5
-- Reference Data
-- We have a csv file WebMetricsTier.csv which contians tier for each metric name--> if tier > 5 that metric is imp to business
-- This csv file is located in a contianer in ADLS
-- Go to the Stream analytics job, create a new input
-- This time, isntead of stream input create a 'reference input'
-- DRop and recreate the output table in Synapse
DROP TABLE WebMetrics
CREATE TABLE WebMetrics(
    Minimum decimal,
    Maximum DECIMAL,
    Average DECIMAL,
    Metrictime DATETIME,
    MetricName VARCHAR(200),
    Tier int
)
-- Start the job and validate
SELECT * FROM WebMetrics


-- Task 6
-- Till now we were using diagnostic settings logs as source. Now we will use NSG flow logs
-- Create a VM---> This also creates a NSG
-- Go to Network Watcher service and create a new flow log which points to the NSG of created VM. 
-- This flow log creates a new directory(insights-logs-networksecuritygroupflowevent) in the container mentioned. (adlsraeez)
-- This log is a Json file which is an array of "records" objects
-- This file is a very complex nested json file
-- Here we also add a UDF (user defined function) in Stream analytics

-- The query to be used in Stream analytics is:
WITH Stage1 as
(
    SELECT
        Records.ArrayValue.time as RecordedTime,
        Records.ArrayValue.properties.flows as flows
    FROM
        nsgflowstream n
    CROSS APPLY GetArrayElements(n.records) as Records
),
STAGE2 as
(
    SELECT 
        RecordedTime, GetArrayElement(flows,0) as flows -- Note the use of 'GetArrayElement' not 'GetArrayElements'
    FROM Stage1 
),
STAGE3 as
(
    SELECT
        S2.RecordedTime,
        S2.flows.[rule] AS Rulename,
        flows
    FROM STAGE2 s2
    CROSS APPLY GetArrayElements(s2.flows.flows) as flows
),
STAGE4 as
(
    SELECT
        S3.RecordedTime,
        S3.Rulename, 
        flowTuples
    FROM STAGE3 S3
    CROSS APPLY GetArrayElements(flows.ArrayValue.flowTuples) as flowTuples
)
SELECT 
    RecordedTime, 
    Rulename, 
    flowTuples.ArrayValue,
    UDF.Getitem(flowTuples.ArrayValue,1) as SourceIpAddress,
    UDF.Getitem(flowTuples.ArrayValue,2) as DestinationIpAddress,
    UDF.Getitem(flowTuples.ArrayValue,4) as SourcePort,
    UDF.Getitem(flowTuples.ArrayValue,7) as Action
FROM 
    STAGE4

-- UDF (Getitem) written in Javascript is:
function main(flowlog, index) {
    var Items = flowlog.split(',');
    return Items[index]
}



-- Task 7
-- We have a container (insights-metrics-pt1m in datastoredp203raeez storage account) that stores metric info from web app service
-- We want ADF to take all the info that gets recorded in this container and transfer it to table Synapse
-- We can use Stream Analytics, but here it is to show how to use ADF to run at specific intervals and take the data
-- If you need real time processing use Event Hubs and Stream Analytics
-- Use ADF to process data in batches

-- we use mapping data flow 
-- FOR NOW, the data flow will take the data from staging container(stagingmetric) and takes it to another container 'output' and from this ouput container the data is taken to table in Synapse

-- Create the staging container 'stagingmetric' and upload a sample json to make the pipeline
-- Create a output staging container 'stagingoutput'
-- Go to ADF ctreate a data flow (dataflow_Metric)
        -- Source = stagingmetric container
        -- Select modifier to select 3 columns
        -- Sink = stagingoutput container

-- Create the pipeline (MetricsFromContainerToSynapse)
-- The pipeline has a data flow activity (as above) and a Copy data activity (to copy data from stagingoutput container to table in Synapse)
-- Create the target table in synapse
DROP TABLE WebMetrics
CREATE TABLE WebMetrics(
    Average DECIMAL,
    MetricTime datetime,
    MetricName VARCHAR(200)
)
SELECT * FROM WebMetrics
DELETE FROM WebMetrics
SELECT COUNT(*) FROM WebMetrics

-- Now we want to delete the files in stagingoutput and stagingmetric containers

-- Our final ADF pipeline should run at specific intervals and take newly added files in the insights-metrics-pt1m container, copies the contents of these files to
-- stagingmetric container, then mapping data flow would take that data and put it to stagingoutput, which is taken by the copy acitvity to push to table in Synapse.
-- Once this entire process done, we need to delete the temp staging files in stagingoutput and stagingmetric containers

-- To ensure the delete, go to the data flow (dataflow_Metric)
-- in the stagingmetricStream---->SourceOptions-->Choose 'Delete source files' in 'After completion' option
-- For stagingoutput, use the delete activity


-- For the pipeline, we first use the Storage Event trigger (run the pipeline whenever a blob created in partivular container stagingmetric)

-- Now we can use tumbling window trigger instead of Storage Event trigger
-- but before that add an activity to copy data from insights-metrics-pt1m container to stagingmetric container.
-- For this copy acitivy, use Copy behaviour=Merge files, this takes all the JSON files in stagingmetric recursively and merge all the contents into just one file
-- Now create the tubling trigger, with recurrence of 15 mins (every 15 mins this pipeline will run)
-- For the current window, the tumbling window trigger will take data coming in that interval, not the data present in previous window
-- This is not possible with schedule trigger, which takes all files and repeats the pipeline
-- Tumbling trigger is perfect if you want to load delta data