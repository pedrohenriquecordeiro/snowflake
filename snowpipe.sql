-- In Snowflake, a pipe is a database object used for continuous data loading. 
-- It automates the process of loading data from an external stage (like AWS S3 or Azure Blob) into a Snowflake table using Snowpipe. 
-- Pipes monitor new files in the stage and load them into the table as soon as they are available, providing real-time or near-real-time data ingestion.

CREATE OR REPLACE PIPE my_snowpipe
AUTO_INGEST = TRUE                           -- Enables automatic file ingestion
AS 
COPY INTO my_table                           -- Target table to load data into
FROM @my_external_stage                      -- External stage where the files are stored
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format') -- File format to parse the files
ON_ERROR = 'CONTINUE';                       -- Continue loading even if some rows have errors

------------------------------------------------------------------------------------------------

-- 1. Describing the Snowpipe to view its configuration and properties
DESCRIBE PIPE my_snowpipe;

-- 2. Pausing the Snowpipe (stops automatic ingestion of new files)
ALTER PIPE my_snowpipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- 3. Resuming the Snowpipe (re-enables automatic ingestion)
ALTER PIPE my_snowpipe SET PIPE_EXECUTION_PAUSED = FALSE;

-- 4. Manually triggering Snowpipe to load files (if AUTO_INGEST is not enabled)
ALTER PIPE my_snowpipe REFRESH;

-- 5. Viewing the history of loads for the Snowpipe (e.g., file loads and statuses)
SELECT * 
FROM table(information_schema.pipe_usage_history(
    PIPE_NAME => 'MY_SNOWPIPE',
    START_TIME => dateadd('day', -7, current_timestamp())));  -- Last 7 days of load history

-- 6. Monitoring Snowpipe activity using the COPY_HISTORY table function
SELECT * 
FROM table(copy_history(TABLE_NAME=>'MY_TABLE', START_TIME=>dateadd('day', -7, current_timestamp())));

-- 7. Dropping the Snowpipe when it's no longer needed
DROP PIPE IF EXISTS my_snowpipe;

-- 8. Creating a notification integration for use with Snowpipe (optional)
CREATE OR REPLACE NOTIFICATION INTEGRATION my_snowpipe_integration
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = 'AWS_SNS'            -- Can use 'AZURE_EVENT_GRID' or 'GCP_PUBSUB' for other cloud platforms
DIRECTION = 'INBOUND'
AWS_SNS_ROLE_ARN = 'arn:aws:iam::your_aws_account_id:role/your_sns_role'
AWS_SNS_TOPIC_ARN = 'arn:aws:sns:region:your_account_id:your_sns_topic';

-- 9. Linking the notification integration to the Snowpipe for automatic triggers
ALTER PIPE my_snowpipe SET INTEGRATION = 'my_snowpipe_integration';
