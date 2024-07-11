-- Create a new databricks workspace in Azure
-- Launch it and get into Databriks UI
-- Go to compute tab and create a cluster

-- Task 1 
-- Laoding daata from a file
-- Now upload a file to DBFS
--When you upload a file to DBFS, the file is stored in the underlying Azure storage account associated with your Databricks workspace.
--DBFS is an abstraction over the underlying Azure storage, It allows you to interact with your data using familiar file system commands.
--When you create an Azure Databricks workspace, an associated storage account is either created automatically or you can attach an existing one.
-- This storage account is where all the data in DBFS is physically stored.
-- group by, filter, date functions, filtering on null values,
-- save the df to table
-- It gets saved onto catalog->database->table

--Task 2
--Reading  data from ADLS
-- access via account keys
-- process JSON


--Task 3
--COPY INTO command