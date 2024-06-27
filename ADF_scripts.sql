-- Below script is different tasks done for the tutorial of ADF

-- Task 1 
-- create a simple pipeline to copy data from 'Log.csv' (in ADLS) into a table in Synapse
-- Below code creates the target table in Synapse
-- DROP TABLE logdata
CREATE TABLE logdata
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time DATETIME,
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000) --- because Resource is a keyword, put this in square brackets
)


-- Now go to ADF, create a pipeline by creating linked services to connect ADF with ADLS and ADF with Synapse
-- Use the Copy data tool to bulk insert data from ADLS container to this table in Synapse
-- Validate teh process by running below query
SELECT * FROM logdata

-- Task 2
-- Copy data from csv contianer to Parquet container

-- Task 3
-- Modify Taks 2
-- Two step activity Pipeline which first copies a csv file (from a container) into a parquet file (in another container). 
-- Also copies the parqeut file to the below table in Synapse
CREATE TABLE logdata_parquet
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time DATETIME,
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000) --- because Resource is a keyword, put this in square brackets
)

SELECT * FROM logdata_parquet



-- Task 4
-- Use a query in copy data tool to transfer data
-- we transfer data present in Azure SQL db to the table below in Synapse

CREATE TABLE dbo.fact_sales
(
    SalesOrderID int NOT NULL,
    OrderDate datetime NOT NULL,
    CustomerID int NOT NULL,
    TaxAmt money NULL,
    OrderQty SMALLINT NOT NULL,
    ProductID int NOT NULL,
    UnitPrice money NOT NULL
)
WITH(
    DISTRIBUTION = HASH(CustomerID) -- hash distribution on ProductID column
)
SELECT * FROM dbo.fact_sales



-- Task 5
-- Adding an extra column
-- Redefne the table below with an extra colmn Filepath
-- add new column while specifying source when defining an activity of a pipeline
-- So in the activity where we copy data from Parqeut contianer to Synapse table below, choose option 'Additional Columns'
DROP TABLE logdata_parquet;
CREATE TABLE logdata_parquet
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time DATETIME,
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000), --- because Resource is a keyword, put this in square brackets
    FilePath VARCHAR(400) NULL -- Addtional column
)
SELECT * FROM logdata_parquet


-- Task 6
-- Copy data using Copy command
-- Note: using addtional columns on COPY command is not supported
-- So avoid the FilePath column
DROP TABLE logdata_parquet;
CREATE TABLE logdata_parquet
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time DATETIME,
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000)--- because Resource is a keyword, put this in square brackets
)
SELECT * FROM logdata_parquet;

-- Task 7 
-- Copy data usinng PolyBase