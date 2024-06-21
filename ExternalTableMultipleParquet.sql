-- Drop the previously created data source for CSV...or can use a new name
-- To drop a data source, you need to drop the corresponding table which uses this data source
-- DROP EXTERNAL TABLE ActivityLog
-- DROP EXTERNAL DATA SOURCE srcActivityLog


-- We create external data source by giving access to the ADLS container using credential
-- First step is to create a credential

--To make use of database scoped credential we need to create master encryption key....This is used internally by  Database engine to encrypt this credential
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssword@123'

-- CREATE THE CREDENTIAL
CREATE DATABASE SCOPED CREDENTIAL sasToken
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2022-11-02&ss=b&srt=sco&sp=rlx&se=2024-06-21T05:05:25Z&st=2024-06-20T21:05:25Z&spr=https&sig=0u5c2cNVALyEguHaGN7UtlYzZQoNYgdkd%2BhHILunbs4%3D'
-- THis secret SAS will be expired....don't try to hack into it mate


CREATE EXTERNAL DATA SOURCE srcActivityLogParquet
WITH(
    LOCATION = 'https://adlsraeez.blob.core.windows.net/parquet/',
    CREDENTIAL = sasToken
)

CREATE EXTERNAL FILE FORMAT parquetFileFormat
WITH(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


CREATE EXTERNAL TABLE ActivityLogP
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time VARCHAR(100),
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000) --- because Resource is a keyword, put this in square brackets
)
WITH(
    LOCATION = '*.parquet', -- Name of the file in container 
    DATA_SOURCE = srcActivityLogParquet, 
    FILE_FORMAT = parquetFileFormat
);

select * from ActivityLogP

