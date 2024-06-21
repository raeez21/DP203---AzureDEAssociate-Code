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
