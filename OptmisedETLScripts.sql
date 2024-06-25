-- This script contains optmised code for creating fact and dimension tables. 
-- Previously all tables were created with Round Robin distribution
-- In here, the fact table is created on Hash Distribution based on Group by column
-- Dimension tables is created as replicated tables
-- These optimisations help us to improve JOINs and Group BY operations


-- Drop the previously created Round Robin fact table
DROP TABLE fct_sales;

-- Recreate the table using hash distributed tables
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
WITH(
    DISTRIBUTION = HASH(ProductID) -- hash distribution on ProductID column
)

-- GO to 'Integrate' section and transfer data again to populate this new fact table
SELECT COUNT(*) FROM fct_sales;

-- RUn the below query to understand the optimisation of using Hash distributed tables
SELECT ProductID, COUNT(SalesOrderID) as 'Count of Sales Orders' FROM fct_sales
GROUP BY ProductID
ORDER BY ProductID


-- Drop the previously created Round Robin dim tables
DROP TABLE dim_customer;
DROP TABLE dim_product;

-- create new replicated dim tables
CREATE TABLE dim_Product
(
    ProductSK int IDENTITY(1,1) NOT NULL, -- using a system generated Surrogate Key column here as unique idfier.........bcoz of DISTRIBUTION, IDENTITY func won't behave as expected
    ProductID int NOT NULL,
    ProductNumber VARCHAR(100) NOT NULL,
    Color VARCHAR(20) NOT NULL,
    ProductCategoryID int NOT NULL,
    ProductCategoryName VARCHAR(200) NOT NULL
)
WITH(
    DISTRIBUTION = REPLICATE
);
SELECT * FROM dim_Product
ORDER BY ProductSK;


CREATE TABLE dim_customer
(
    CustomerID int NOT NULL,
    CompanyName VARCHAR(500) NOT NULL
)
WITH(
    DISTRIBUTION = REPLICATE
);

-- Now Go to 'Integrate' tab and perform the COPY operation

SELECT COUNT(*) from dim_customer
SELECT COUNT(*) from dim_Product



-- JOIN operation on fact and dim tables
SELECT fs.ProductID, COUNT(dp.Color) as 'Color Count'
FROM fct_sales fs
JOIN dim_Product dp
ON fs.ProductID = dp.ProductID
GROUP BY fs.ProductID


-- TO see the distribution of table, use below commands
DBCC PDW_SHOWSPACEUSED('fct_sales')
