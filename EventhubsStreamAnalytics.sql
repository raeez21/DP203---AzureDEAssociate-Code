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