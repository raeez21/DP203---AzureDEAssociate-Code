-- Create a new database, we no longer use the default master DB in Synapse because it is for system based tables
CREATE DATABASE [appdb]

-- Create a new data source which points to our container in ADLS (Note that the path doesnt contain the file name)
CREATE EXTERNAL DATA SOURCE srcActivityLog
WITH(
    LOCATION = 'https://adlsraeez.blob.core.windows.net/csv/'
)

-- Creating a file format of CSV
CREATE EXTERNAL FILE FORMAT delimitedTextFormat
WITH(
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2
    )
);

-- Create a new external table
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
    LOCATION = 'Log.csv', -- Name of the file in container 
    DATA_SOURCE = srcActivityLog, 
    FILE_FORMAT = delimitedTextFormat
);

select * from ActivityLog