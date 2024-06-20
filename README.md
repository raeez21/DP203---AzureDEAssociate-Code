# DP203 AzureDEAssociate-Code

This repo contains main code utilised while preparing for DP 203 course Azure Data Engineer Associate Certification

# Synapse
 One of the initial task was to read files contained in an ADLS container from Synapse with SQL queries using Serverless SQL pools. This was done by creating External tables (Synapse manages the metadata but the underlying data is still stored in ADLS container).
 
 1. [ExternalTableFromCsvAdls.sql](ExternalTableFromCsvAdls.sql)

    Contains the code to create an external table which points to a csv file in an ADLS container. The access here was given using IAM role (Storage Blob Reader) in the ADLS container UI to the user.

    Here we created a new database, a new data source, a new file format and finally the external table

2. [ExternalTableFromParquetAdls.sql](ExternalTableFromParquetAdls.sql)

    This contains SQL code to create external table pointing to a parquet file in ADLS. The access was given using Credentials (SAS token).

    Here we created a master encryption key, then the scoped credential, data source (which uses this creds and sas token), file format (parquet) and finally the external table
