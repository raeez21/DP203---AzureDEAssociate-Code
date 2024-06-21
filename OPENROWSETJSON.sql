SELECT 
    JSON_VALUE(jsonContent, '$.Correlationid') as Correlationid,
    JSON_VALUE(jsonContent, '$.Operationname') as Operationname,
    JSON_VALUE(jsonContent, '$.Status') as Status,
    JSON_VALUE(jsonContent, '$.Eventcategory') as Eventcategory
    FROM OPENROWSET(
    BULK 'https://adlsraeez.blob.core.windows.net/json/Log.json',
    FORMAT = 'csv',
    -- Below parameters are needed to signify how the csv file is structured as a json based file
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0a'
)
WITH(
    -- The column name which stores all the info returned by OPENROWSET funciton
    jsonContent VARCHAR(MAX)
)
AS ROWS