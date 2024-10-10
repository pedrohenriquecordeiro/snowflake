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


-- Loading data into the target table with data transformations
COPY INTO my_table
FROM (
    SELECT
        TRY_CAST(TRIM($1) AS VARCHAR(100)) AS first_name,   -- Trim and cast the first column to VARCHAR
        TRY_CAST(TRIM($2) AS VARCHAR(100)) AS last_name,    -- Trim and cast the second column to VARCHAR
        TRY_CAST(TRIM($3) AS DATE) AS birth_date,           -- Trim and cast the third column to DATE
        TRY_CAST(TRIM($4) AS INTEGER) AS age,               -- Trim and cast the fourth column to INTEGER
        TRY_CAST(TRIM($5) AS DECIMAL(10, 2)) AS salary,     -- Trim and cast the fifth column to DECIMAL
        TRY_CAST(TRIM($6) AS BOOLEAN) AS is_active          -- Trim and cast the sixth column to BOOLEAN
    FROM @my_external_stage
    (FILE_FORMAT => 'my_csv_format')
)
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
ON_ERROR = 'CONTINUE';    -- Continue loading even if some rows contain errors
