-- This script contains code to create tables which will store fact and dim tables
-- The source for these tables are in Azure SQL DB tables and we pull the data using Pipelines (Integrate tab on Synapse studio)

CREATE TABLE fct_sales
(
    SalesOrderID int NOT NULL,
    OrderDate datetime NOT NULL,
    CustomerID int NOT NULL,
    SubTotal money NOT NULL,
    TaxAmt money NOT NULL,
    Freight money NOT NULL,
    TotalDue money NOT NULL,
    OrderQty int,
    ProductID int NOT NULL,
    UnitPrice money NOT NULL,
    UnitPriceDiscount money NOT NULL,
    LineTotal decimal NOT NULL 
)

CREATE TABLE dim_Product
(
    ProductID int NOT NULL,
    ProductNumber VARCHAR(100) NOT NULL,
    Color VARCHAR(20) NOT NULL,
    ProductCategoryID int NOT NULL,
    ProductCategoryName VARCHAR(200) NOT NULL
);

CREATE TABLE dim_customer
(
    CustomerID int NOT NULL,
    CompanyName VARCHAR(500) NOT NULL
);

-- After Creating these tables in Synapse, now go create the Pipeline in Integration table
-- While specifying source Instead of 'Tables' choose 'Query' and copy below SQL stmts.
-- We have 1 fct and 2 dim tables...so repeat this activity 3 times one for each table

--FOr sales fact table
SELECT hd.SalesOrderID, hd.CustomerID, hd.OrderDate, hd.SubTotal, hd.TaxAmt, hd.Freight, hd.TotalDue,
dt.OrderQty, dt.ProductID, dt.UnitPrice, dt.UnitPriceDiscount, dt.LineTotal
FROM SalesLT.SalesOrderHeader hd 
INNER JOIN SalesLT.SalesOrderDetail dt
ON hd.SalesOrderID = dt.SalesOrderID

-- For Customer dim table
select CustomerID, CompanyName from SalesLT.Customer; -- Dimension Table 1


-- For product dim table
SELECT pd.ProductID, pd.ProductNumber, pd.Color, pd.ProductCategoryID, ct.Name as 'ProductCategoryName'
FROM SalesLT.Product pd
INNER JOIN SalesLT.ProductCategory ct
ON pd.ProductCategoryID = ct.ProductCategoryID
where pd.Color IS NOT NULL



-- After all 3 pipelines are successfull, check the table creation:
SELECT * FROM dbo.fct_sales
select * from dim_customer
select * from dim_product

