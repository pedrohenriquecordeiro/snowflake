-- In Snowflake, a stage is a location used to store data files temporarily before loading them into tables or after unloading data. 
-- Stages can be internal (within Snowflake) or external (cloud storage like AWS S3, Azure Blob, or Google Cloud Storage). 
-- It acts as an intermediary for data ingestion or export.

CREATE OR REPLACE STAGE my_external_stage
URL = 's3://your-bucket-name/path/'
STORAGE_INTEGRATION = my_s3_integration       -- Uses the storage integration created for S3
FILE_FORMAT = my_csv_format                   -- Uses the file format for handling CSVs
COPY_OPTIONS = (
    ON_ERROR = 'CONTINUE',                    -- Specifies how to handle errors (e.g., continue on error)
    FORCE = TRUE                              -- Forces loading even if the data is already loaded
);

--------------------------------------------------------------------------------------------------------

-- 1. Describing the stage to view its properties
DESCRIBE STAGE my_external_stage;

-- 2. Listing the files in the external stage directory
LIST @my_external_stage;

-- 3. Querying data directly from the external stage without loading into a table
SELECT 
    $1 AS column1, -- first column from files csv
    $2 AS column2, -- second column from files csv
    $3 AS column3  -- third column from files csv
FROM 
    @my_external_stage
    (FILE_FORMAT => 'my_csv_format');

-- 4. Loading data from the external stage into the table
COPY INTO my_table
FROM @my_external_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
ON_ERROR = 'CONTINUE';

-- 5. Unloading data from a table to the external stage (i.e., exporting data to S3)
COPY INTO @my_external_stage/unload_directory/
FROM my_table
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
HEADER = TRUE
OVERWRITE = TRUE;

-- 6. Listing files in the stage after the unload operation
LIST @my_external_stage/unload_directory/;

-- 7. Removing specific files from the stage
REMOVE @my_external_stage/unload_directory/prefix_to_delete_;

-- 8. Dropping the external stage when no longer needed
DROP STAGE IF EXISTS my_external_stage;
