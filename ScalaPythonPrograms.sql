-- Install JDK and Scala into windows machine.
-- Open a terminal and give below code

-- sbt new scala/scala3.g8
-- This creates a skeleton of scala project using scala version 3
-- Name the project folder and open it in Visual Studio Code

-- got to src/main/scala/Main.scala and run the below command
-- scala Main.scala
-- You can see the outputs in cmd


-- Create a new Spark pool from the Synapse workspace page in Azure
-- Create new notebook 'NotebookBasics'



--In this task we take data files from ADLS and put them into a table in synapse dedciated SQL pool
SELECT * FROM logdata_parquet
DELETE FROm logdata_parquet

--internaldb.logdatanew is a table created in Spark pool in a notebook whihc can be acessed from serverless SQL pool
-- Change the 'Connect to' above to 'Built-in' to use the serverless SQL pool and choose 'database' as 'internaldb'
-- Give the below command and show its working from serverless sql pool
SELECT * FROM logdatanew



