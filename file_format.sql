
-- In Snowflake, a file format is a configuration that defines how data is read from external files (e.g., CSV, JSON, Parquet). 
-- It specifies options like delimiters, encoding and  compression, allowing Snowflake to correctly parse and load the file data.

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'                                  -- Specifies the file type as CSV
FIELD_DELIMITER = ';'                         -- Uses semicolon (;) as the field delimiter
RECORD_DELIMITER = '\n'                       -- Newline (\n) used to separate records (rows)
SKIP_HEADER = 1                               -- Skips the first line, treating it as a header
FIELD_OPTIONALLY_ENCLOSED_BY = '"'            -- Fields can optionally be enclosed by double quotes (")
ESCAPE_UNENCLOSED_FIELD = '\\'                -- Backslash (\) is the escape character for unquoted fields
NULL_IF = ('\\N', 'NULL', '')                 -- Treats the values '\N', 'NULL', and empty strings as NULL
EMPTY_FIELD_AS_NULL = TRUE                    -- Treats empty fields as NULL
TRIM_SPACE = TRUE                             -- Removes leading and trailing spaces from fields
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE         -- Throws an error if the number of columns in the file doesn't match the target table
DATE_FORMAT = 'AUTO'                          -- Automatically detects date format based on file content
TIMESTAMP_FORMAT = 'AUTO'                     -- Automatically detects timestamp format based on file content
ENCODING = 'UTF8'                             -- Specifies UTF8 encoding for the file
COMPRESSION = 'AUTO';                         -- Automatically detects file compression type (e.g., gzip, bz2)

---------------------------------------------------------------------------------------------------------------

-- 1. Describing a file format to view its properties
DESCRIBE FILE FORMAT my_csv_format;

-- 2. Listing all file formats in the current schema
SHOW FILE FORMATS;

-- 3. Altering an existing file format (e.g., changing field delimiter)
ALTER FILE FORMAT my_csv_format 
SET FIELD_DELIMITER = ',',
    COMMENT = 'Updated CSV file format with comma delimiter';

-- 4. Dropping the JSON file format
DROP FILE FORMAT IF EXISTS my_json_format;
