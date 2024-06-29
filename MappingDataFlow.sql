SELECT * FROM dbo.fact_sales

-- Only delete the target table, dont drop it
DELETE FROM dbo.fact_sales

-- Go to ADF-->Author tab--->Data flows--->New data flow
-- SalesOrderDetail  is the main table in the below query, so use that as the first source in data flow creation
-- Add another source below it pointing to SalesOrderHeader


-- SELECT hd.SalesOrderID, hd.OrderDate, hd.CustomerID,hd.TaxAmt, dt.OrderQty, dt.ProductID, dt.UnitPrice
-- FROM SalesLT.SalesOrderDetail dt 
-- INNER JOIN SalesLT.SalesOrderHeader dh
-- ON hd.SalesOrderID = dt.SalesOrderID


-- After the data flow is created, how do we run it? (We dont have a trigger button)
-- For this we have to create a new pipeline and drag the Data flow tool into the Canvas
-- go to settings tab and under Data Flow, choos the created one
-- Publish it and run this pipeline
-- This would take some time as it have to spin up Spark Cluster
SELECT * FROM dbo.fact_sales


-- Similarly we build the other two dim table
-- We use one mapping data flow  for both the dim tables
-- dim_customer
select * from dbo.dim_customer
DROP TABLE dbo.dim_customer
CREATE TABLE dim_customer
(
    CustomerID int NOT NULL,
    CompanyName VARCHAR(200) NOT NULL,
    SalesPerson VARCHAR(300) NOT NULL
)
WITH(
    DISTRIBUTION = REPLICATE
);
-- Use below query to draw the canvas in Data Flow
--SELECT ct.CustomerID, ct.CompanyName, ct.SalesPerson, FROM SalesLT.Customer as ct


-- dim_product
select * from dim_product;
drop table dim_product;
CREATE TABLE dim_Product
(
    ProductID int NOT NULL,
    ProductModelID INT NOT NULL,
    ProductcategoryID int NOT NULL,
    ProductName VARCHAR(50) NOT NULL,
    ProductModelName VARCHAR(50) NULL,    
    ProductCategoryName VARCHAR(200) NOT NULL
)
WITH(
    DISTRIBUTION = REPLICATE
);

-- Data flow canvas designed with below query
-- SELECT prod.ProductID, prod.Name as ProductName, model.ProductModelID, model.ProductModelName, category.ProductcategoryID, category.ProductCategoryName
-- FROM SalesLT.Product prod
-- LEFT JOIN SalesLT.ProductModel model on prod.ProductModelID = model.ProductModelID
-- LEFT JOIN SalesLT.ProductCategory category on prod.ProductCategoryID = category.ProductCategoryID



-- Create derived column with dynamic values in fact_sales table
DROP TABLE fact_sales;
CREATE TABLE fact_sales
(
    ProductID int NOT NULL,
    SalesOrderID int NOT NULL,
    CustomerID int NOT NULL,
    OrderQty smallint not null,
    UnitPrice money NOT NULL,
    OrderDate datetime NULL,
    TaxAmt money NOT NULL,
    TotalAmount money not null  -- NEw derived column
)
WITH(
    DISTRIBUTION = HASH(CustomerID) -- hash distribution on ProductID column
)

-- Now go tot 'MappingDataFlow_FactSales' Pipeline in ADF
-- In that pipeline, after the join activity use a 'Schema Modifier' of 'select' to only select columns of interest
-- After that choose 'Derived Column' of Schema Modifier and use the expression builder to create the derivde column
-- TotalAmount = UnitPrice * OrderQty 
-- Now in the final Sink, reset the mapping and add a new mapping of TotalAmount
-- Before this you have to refresh the sink dataset (that connects to Synapse table above), since it was changed
SELECT * FROM fact_sales


-- Add a Surrogate Key to the Dim table using Mapping data flow
select * from dim_Product
DROP TABLE dim_Product

CREATE TABLE dim_Product
(
    ProductSK int not null,
    ProductID int NOT NULL,
    ProductModelID INT NOT NULL,
    ProductcategoryID int NOT NULL,
    ProductName VARCHAR(50) NOT NULL,
    ProductModelName VARCHAR(50) NULL,    
    ProductCategoryName VARCHAR(200) NOT NULL
)
WITH(
    DISTRIBUTION = REPLICATE
);

-- Go tot dataflow_dimensions workflow in ADF
-- Create a Select shchema modifier after the join activity like above, and select the appropiate wanted columns
-- Now choose the Surrogate Key option of Schema modifier
-- Now in the final Sink, reset the mapping and add a new mapping of ProductSK
-- Before this you have to refresh the sink dataset (that connects to Synapse table above), since it was changed
DELETE FROM dim_customer
-- Now run the MappingDataFlow_dim Pipeline in ADF
select * from dim_Product
order by ProductSK  -- Here the SK is populated in proper incremental order unlike the previous method of Using Syanpse pipelines where the distributiion spoilt the order of SK generated

-- Cache Sink and Lookup
-- Add a SK column in dim_customer

DROP TABLE dbo.dim_customer;
CREATE TABLE dim_customer
(
    CustomerSK int not null,
    CustomerID VARCHAR(200) NOT NULL,
    CompanyName VARCHAR(200) NOT NULL,
    SalesPerson VARCHAR(300) NOT NULL
)
WITH(
    DISTRIBUTION = REPLICATE
);
DELETE FROM dim_Product
-- GO to dataflow_dimensions data flow on ADF and create a select Schema modifier bewteen
-- Then add Surrgotae Key Schema modifier 
-- Before this go to the final sink and refresh the mapping to include the CustomerSK
-- Now to stimulate multiple loads, we are breaking the Customer table in Azure SQL DB to 2 csv files
-- Customer01.csv has rows where CustomerID from 1 to 468
-- Customer02.csv has rows where CustomerID from 469 to end
-- These two files are now stored in a ADLS contianer--->    adlsraeez->csv->Customers->
-- So now the dataset in the source needs to be changed from Azure SQL DB to ADLS container
-- While defining the dataset first point to Customer01.csv in the first load
-- unitl now Loading of Customer01.csv  is complete

SELECT * FROM dim_customer
order by CustomerSK
-- After every load we need to store the max value of CustomerSK onto cache
-- Now create a new source in the canvas of data flow called GETMAXCustomerSK
-- GETMAXCustomerSK takes the dim_customer table of Synapse as its source dataset
-- Now go to Source Options of this source and choose query isntead of table and give below query
-- SELECT MAX(CustomerSK) as CustomerSK from dim_customer

-- Now onto this source activity add a SINK, CustomerSKSink
-- This SINK will be adding to cache of the Spark clusters instead of any data store
-- while defining the SINK in Sink type option choose Cache
-- Now go to GETMAXCustomerSK, and click 'import projection' --> this turns on and creates a Data Flow Debug
-- nOw go to Projection tab and click 'import projection'
-- After that is complete toggle off Data Flow Debug on the top left corner of Canvas

-- GO TO CustomerSKSink--->Mapping and validate the Mapping of CustomerSK
-- So here we take the max value of CustomerSK and temporary write in to a cache location of Spark clusters



-- Now go to CustomerSKStream and add a derived column from there
-- and create a expression belwo using Expression builder

--CustomerSK + CustomerSKSink#outputs()[1].CustomerSK
  -- During the first load (Customer01.csv) CustomerSK value is 1 and CustomerSKSink#outputs()[1].CustomerSK value is undefined, so inital value of CustomerSK will be 1
  -- In the second load (Customer02.csv), CustomerSK value is 1 and CustomerSKSink#outputs()[1].CustomerSK value is 299 (the max value cached from previous load), so new value 
  -- of CusotmerSK is 300

SELECT * FROM dim_customer
order by CustomerSK

SELECT COUNT(*) FROM dim_Product