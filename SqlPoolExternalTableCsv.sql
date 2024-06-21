CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssword@123'

-- CREATE THE CREDENTIAL
CREATE DATABASE SCOPED CREDENTIAL sasToken2
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2022-11-02&ss=b&srt=sco&sp=rlx&se=2024-06-21T22:06:16Z&st=2024-06-21T14:06:16Z&spr=https&sig=KiHM9WixfjcQHh3CY%2FmyvgAzk5AlEMiYyndCfmBorEk%3D'

CREATE EXTERNAL DATA SOURCE srcActivityLogCsv
WITH(
    LOCATION = 'abfss://csv@adlsraeez.blob.core.windows.net', --since we using Hadoop driver, there is a change of url,(note the use of abfss protocol)
    TYPE = HADOOP, -- Since this is a CSV file we need to create a hadoop external table, so use this driver
    CREDENTIAL = sasToken2
)

--File format remains same as of serverless sql pool
CREATE EXTERNAL FILE FORMAT delimitedTextFormat
WITH(
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2
    )
);


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
    DATA_SOURCE = srcActivityLogCsv, 
    FILE_FORMAT = delimitedTextFormat
);

select * FROM ActivityLog