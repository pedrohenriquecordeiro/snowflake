
-- In Snowflake, storage integration is a configuration that securely connects Snowflake to external cloud storage, like AWS S3, Azure Blob Storage, or Google Cloud Storage. 
-- It enables Snowflake to access and load data directly from these external storage systems while managing permissions and security.

SHOW INTEGRATIONS;

-- aws s3
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your_aws_account_id:role/your_snowflake_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name');

-- gcp cloud storage
CREATE STORAGE INTEGRATION my_gcs_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
ENABLED = TRUE
STORAGE_GCP_SERVICE_ACCOUNT = 'your-service-account@your-project-id.iam.gserviceaccount.com'
STORAGE_ALLOWED_LOCATIONS = ('gcs://your-bucket-name');

-- azure blob storage
CREATE STORAGE INTEGRATION my_blob_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'AZURE'
ENABLED = TRUE
STORAGE_AZURE_TENANT_ID = 'your-azure-tenant-id'
STORAGE_AZURE_CLIENT_ID = 'your-azure-client-id'
STORAGE_AZURE_CLIENT_SECRET = 'your-azure-client-secret'
STORAGE_ALLOWED_LOCATIONS = ('azure://your-account-name.blob.core.windows.net/your-container-name');

DESCRIBE INTEGRATION my_s3_integration;

DROP INTEGRATION my_s3_integration;
