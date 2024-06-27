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