# DP203 AzureDEAssociate-Code

This repo contains main code utilised while preparing for DP 203 course Azure Data Engineer Associate Certification

# 1. Synapse
 One of the initial task was to read files contained in an ADLS container from Synapse with SQL queries using Serverless SQL pools. This was done by creating External tables (Synapse manages the metadata but the underlying data is still stored in ADLS container).
 ## Serverless SQL pools
 1. [ExternalTableFromCsvAdls.sql](ExternalTableFromCsvAdls.sql)

    Contains the code to create an external table which points to a csv file in an ADLS container. The access here was given using IAM role (Storage Blob Reader) in the ADLS container UI to the user.

    Here we created a new database, a new data source, a new file format and finally the external table

2. [ExternalTableFromParquetAdls.sql](ExternalTableFromParquetAdls.sql)

    This contains SQL code to create external table pointing to a parquet file in ADLS. The access was given using Credentials (SAS token).

    Here we created a master encryption key, then the scoped credential, data source (which uses this creds and sas token), file format (parquet) and finally the external table

3. [ExternalTableMultipleParquet.sql](ExternalTableMultipleParquet.sql)
   
   This is same as previous one except it reads all parqeut file in a container to the table. This is done by use of wildcard character '*' in LOCATION field of CREATE TABLE STATEMENT. Rest everything remains same as of 2.

5. [OPENROWSETJSON.sql](OPENROWSETJSON.sql)

   This file contains the usage of OPENROWSET function which allows to access files in Azure Storage. This function reads contents of a remote data source (for ex: a file) amd returns content as set of rows. Access in this file is given using IAM role. Make note of parameters used like FIELDTERMINATOR, FIELDQUOTE etc.
   
## Dedicated SQL pools

 To host a SQL DW we need to make use of dedicated sql pools to persist the data. Dedicated SQL pools can also be used to create external tables, and first few files below shows that.

 1. [SqlPoolExternalTableParquet.sql](SqlPoolExternalTableParquet.sql)

    This program creates an external table from a Parquet file in ADLS using dedicated SQL pool. So create a dedidcated SQL pool and then connect to it using 'Connect to' dropdown in SQL editor of Synapse.
    Since this from Parquet file, we can create Native External tables. So the codes remain almost the same.

 2. [SqlPoolExternalTableCsv.sql](SqlPoolExternalTableCsv.sql)

    This script creates an external table from a CSV file in ADLS using dedicated SQL pool. Since it is CSV, we need to create Hadoop External table. So there are few changes like:

    - using abfss protocol while specifiying LOCATION while defining EXTERNAL DATA SOURCE
    - 'TYPE' is given as 'HADOOP'
 ## Loading Data to dedicated SQL pools

 We can create normal persisted table on dedicated SQL pool and load data using various ways.
 1. [LoadDataToNormalTable.sql](LoadDataToNormalTable.sql)

    This file shows various ways:

      - Create a new normal table using existent external table using CTAS stmt. The underlying technology used here is Polybase
      - Creating a normal table using COPY INTO command which loads data from CSV file in ADLS
      - Creating a normal table using COPY INTO command which loads data from Parquet file in ADLS
      - Load data using 'Pipeline' which loads data from file in ADLS container
      - use 'Pipelines' to transfer data from table in a relational DB(Azure SQL DB) to table in dedicated SQL Pool (Data Warehouse).
   
 2. [ETL_Scripts.sql](ETL_Scripts.sql)

    Our Azure SQL DB acts as a OLTP database from which we pull data into a DW. The data in DW is in dimensional model (facts and dimensions).
    This script contains code to create tables which will store fact and dim tables. The source for these tables are in Azure SQL DB tables and we pull the data using Pipelines (Integrate tab on Synapse studio)

    After Creating these tables in Synapse, now go create the Pipeline in Integration table. While specifying source Instead of 'Tables' choose 'Query' and copy SQL stmts found in the last section of this script. We have 1 fct and 2 dim tables...so repeat this activity 3 times one for each table


3. [OptmisedETLScripts.sql](OptmisedETLScripts.sql)
   
    This script contains some optimised code icnluding hash distributed fact tables and replicated dimension tables. It also depicts the use of Surrogate Keys (SKs)

   TODO: Do [TablePartitions.sql](TablePartitions.sql) and Synapse archi
   
