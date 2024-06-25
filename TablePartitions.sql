SELECT * FROM PoolActivityLogCpParquet;
-- The above table has 'Time' col as VARCHAR...we need this in DATE format
-- So drop this table and recreate
DROP TABLE PoolActivityLogCpParquet;

CREATE TABLE PoolActivityLogCpParquet
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time DATETIME, -- this column is in datetime
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000) --- because Resource is a keyword, put this in square brackets
)

-- Now go to 'Integrate' tab and run the Pipeline 'Pipeline_Adls_To_SynapseTable'
-- Execute below query, you can see data is spread across multiple data values, so we can partition on that column
SELECT FORMAT(TIme, 'yyyy-MM-dd') AS 'Operation DateTime', COUNT(Operationname) AS 'NUmber of Operations'
FROM PoolActivityLogCpParquet
GROUP BY FORMAT(TIme, 'yyyy-MM-dd')
ORDER BY 'Operation DateTime';


-- Drop the table and recreate the table with partition

DROP TABLE PoolActivityLogCpParquet;
CREATE TABLE PoolActivityLogCpParquet
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time DATETIME, -- this column is in datetime
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000) --- because Resource is a keyword, put this in square brackets
)
WITH(
    DISTRIBUTION = HASH(Operationname), -- distribution
    PARTITION(
        TIME RANGE RIGHT FOR VALUES -- Notice the partition
        ('2023-03-01', '2023-04-01') --  these are the boundary values in that column
        -- Totally this data has values for Month April, March and everything before March
        -- Partition 1 will be everything beyond mArch, Part 2 is March, Part 3 is April 
    )
);


SELECT FORMAT(TIme, 'yyyy-MM-dd') AS 'Operation DateTime', COUNT(Operationname) AS 'NUmber of Operations'
FROM PoolActivityLogCpParquet
GROUP BY FORMAT(TIme, 'yyyy-MM-dd')
ORDER BY 'Operation DateTime';


-- Showing how Parition Switiching works

CREATE TABLE PoolActivityLog_new
(
    Correlationid VARCHAR(200),
    Operationname VARCHAR(300),
    [Status] VARCHAR(100), --- because Status is a keyword, put this in square brackets
    Eventcategory VARCHAR(100),
    Level VARCHAR(100),
    Time DATETIME, -- this column is in datetime
    [Subscription] VARCHAR(200), --- because subscription is a keyword, put this in square brackets
    Eventinitiatedby VARCHAR(1000),
    Resourcetype VARCHAR(300),
    Resourcegroup VARCHAR(1000),
    [Resource] VARCHAR(2000) --- because Resource is a keyword, put this in square brackets
)
WITH(
    DISTRIBUTION = HASH(Operationname), -- distribution
    PARTITION(
        TIME RANGE RIGHT FOR VALUES -- Notice the partition
        ('2023-04-01') --  This means, partition 1 is for all values before April and Partition 2 is for Month of April
    )
);

-- Move the partition 2 (Month of March) of main table to partition 1 (everything before April) of new table
ALTER TABLE PoolActivityLogCpParquet SWITCH PARTITION 2 TO PoolActivityLog_new PARTITION 1;

SELECT FORMAT(TIme, 'yyyy-MM-dd') AS 'Operation DateTime', COUNT(Operationname) AS 'NUmber of Operations'
FROM PoolActivityLogCpParquet
GROUP BY FORMAT(TIme, 'yyyy-MM-dd')
ORDER BY 'Operation DateTime';


SELECT FORMAT(TIme, 'yyyy-MM-dd') AS 'Operation DateTime', COUNT(Operationname) AS 'NUmber of Operations'
FROM PoolActivityLog_new
GROUP BY FORMAT(TIme, 'yyyy-MM-dd')
ORDER BY 'Operation DateTime';