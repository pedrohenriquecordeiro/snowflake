
-- In Snowflake, COPY INTO is a command used to load data into a table from an external stage or unload data from a table to an external stage. 
-- It specifies the data source, file format, and error handling options for importing or exporting data, 
-- making it a key part of managing data transfers between Snowflake and external storage systems.

-- Basic COPY INTO to load data from an external stage into a table
COPY INTO my_table
FROM @my_external_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');


-- COPY INTO with additional options for loading
COPY INTO my_table
FROM @my_external_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
PATTERN = '.*\\.csv'                                  -- Load only CSV files using a regex pattern
ON_ERROR = 'CONTINUE'                                 -- Continue loading if some rows have errors
PURGE = TRUE                                          -- Delete files from the stage after successful load
FORCE = TRUE                                          -- Reload data even if files were already loaded
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE               -- Match columns by name (ignoring case) instead of position
VALIDATION_MODE = RETURN_ERRORS                       -- Only validate files, return errors without loading data
CREDENTIALS = (AWS_KEY_ID = 'your-aws-key-id', AWS_SECRET_KEY = 'your-secret-key'); -- Provide credentials if required


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


