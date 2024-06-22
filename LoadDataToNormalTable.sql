-- Create a new normal table using existent external table activitylog using CTAS stmt
-- The underlying technology used here is Polybase
-- CTAS stmt can take the schema from base table, so no need to explicitly declare new table
CREATE TABLE PoolActivityLogPoly
WITH(
    DISTRIBUTION = ROUND_ROBIN
)
AS
select * from ActivityLog;


select * from PoolActivityLogPoly
select count(*) from PoolActivityLogPoly



-- Creating a normal table using COPY INTO
-- here we need to first declare the target table
CREATE TABLE PoolActivityLogCP
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
    DISTRIBUTION = ROUND_ROBIN
)
-- Copy Into command
COPY INTO PoolActivityLogCP
FROM 'https://adlsraeez.blob.core.windows.net/csv/Log.csv' --Location of csv file in ADLS
WITH(
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
    CREDENTIAL = (
        IDENTITY = 'Shared Access Signature',
        SECRET = 'sv=2022-11-02&ss=b&srt=sco&sp=rlyx&se=2024-06-22T20:54:29Z&st=2024-06-22T12:54:29Z&spr=https&sig=EaLt7CupaLLZSUQJDEBiLiXsr%2B3tqXyXdZ2fcqKZ6%2F0%3D'
    )
);
SELECT * FROM PoolActivityLogCP
SELECT count(*) from PoolActivityLogCP



-- ABove was using CSV file, now use Parquet files to create table using COPY INTO
CREATE TABLE PoolActivityLogCpParquet
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
    DISTRIBUTION = ROUND_ROBIN
);

COPY INTO PoolActivityLogCpParquet
FROM 'https://adlsraeez.blob.core.windows.net/parquet/log.parquet' --Location of parquet file in ADLS
WITH(
    FILE_TYPE = 'PARQUET', --file type is PARQUET
    CREDENTIAL = (
        IDENTITY = 'Shared Access Signature',
        SECRET = 'sv=2022-11-02&ss=b&srt=sco&sp=rlyx&se=2024-06-22T20:54:29Z&st=2024-06-22T12:54:29Z&spr=https&sig=EaLt7CupaLLZSUQJDEBiLiXsr%2B3tqXyXdZ2fcqKZ6%2F0%3D'
    )
);
SELECT * FROM PoolActivityLogCpParquet
SELECT count(*) FROM PoolActivityLogCpParquet



-- Using Pipelines to load data
DELETE FROM PoolActivityLogCpParquet --DELETE the contents from the table

-- Now go to Integrate section on left bar of Synapse studio and select COPY DATA TOOL option
-- We connect to Adls container using Linked service



-- We can use 'Pipelines' to transfer data from table in a relational DB(Azure SQL DB) to table in dedicated SQL Pool (Data Warehouse).
-- Note: Make sure firewall in Azure SQL DB is not blocking the connection from Syanspe
-- After the pipeline is success a new corresponding table is created inside the SQL pool database (in our case 'datapool')