-- In Snowflake, unloading refers to the process of exporting data from a Snowflake table to an external stage (such as AWS S3, Azure Blob, or Google Cloud Storage). 
-- This allows you to move data out of Snowflake in a structured format (like CSV, JSON, or Parquet) for backup, sharing, or further processing outside of Snowflake. 
-- The COPY INTO command is used for unloading data.


-- Basic COPY INTO to unload data from a table into an external stage
COPY INTO @my_external_stage/unload_directory/
FROM my_table
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');


-- Unloading data with additional options
COPY INTO @my_external_stage/unload_directory/
FROM my_table
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
HEADER = TRUE                                          -- Include headers in the unloaded CSV files
OVERWRITE = TRUE                                       -- Overwrite existing files in the stage
PARTITION_BY = TO_CHAR(current_date, 'YYYY-MM-DD')     -- Partition the output by date
INCLUDE_QUERY_ID = TRUE                                -- Add the query ID to the output file names
SINGLE = FALSE                                         -- Split data into multiple files (default is multiple)
MAX_FILES = 10                                         -- Limit the number of output files to 10
CREDENTIALS = (AWS_KEY_ID = 'your-aws-key-id', AWS_SECRET_KEY = 'your-secret-key'); -- Provide credentials if needed


-- Unloading a subset of data from a table to an external stage using a SELECT statement
COPY INTO @my_external_stage/unload_directory/
FROM (SELECT column1, column2, column3 
      FROM my_table
      WHERE column1 > 100)                   -- Only unload rows where column1 is greater than 100
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
HEADER = TRUE                                -- Include headers in the exported CSV files
OVERWRITE = TRUE;                            -- Overwrite any existing files in the stage
