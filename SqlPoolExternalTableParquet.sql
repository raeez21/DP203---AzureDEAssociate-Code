-- This script creates an external table from a Parquet file in ADLS using dedicated SQL pool
-- So create a dedidcated SQL pool and then connect to it using 'Connect to' dropdown in SQL editor of Synapse

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssword@123'

-- CREATE THE CREDENTIAL
CREATE DATABASE SCOPED CREDENTIAL sasToken
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2022-11-02&ss=b&srt=sco&sp=rlx&se=2024-06-21T05:05:25Z&st=2024-06-20T21:05:25Z&spr=https&sig=0u5c2cNVALyEguHaGN7UtlYzZQoNYgdkd%2BhHILunbs4%3D'

-- creating data source
CREATE EXTERNAL DATA SOURCE srcActivityLog1
WITH(
    LOCATION = 'https://adlsraeez.blob.core.windows.net/parquet/',
    CREDENTIAL = sasToken
)
-- creating file format
CREATE EXTERNAL FILE FORMAT parquetFileFormat
WITH(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
-- creating extenral table
CREATE EXTERNAL TABLE ActivityLog
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
    LOCATION = 'log.parquet', -- Name of the file in container 
    DATA_SOURCE = srcActivityLog1, 
    FILE_FORMAT = parquetFileFormat
);

SELECT * FROM ActivityLog
