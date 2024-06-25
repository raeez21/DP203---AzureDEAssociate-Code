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

SELECT ProductID, COUNT(SalesOrderID) as 'Count of Sales Orders' FROM fct_sales
GROUP BY ProductID
ORDER BY ProductID