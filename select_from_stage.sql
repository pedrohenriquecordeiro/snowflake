
SELECT
    TRY_CAST(TRIM($1) AS VARCHAR(100)) AS first_name,      -- Trim and try to cast the first attribute to VARCHAR
    TRY_CAST(TRIM($2) AS VARCHAR(100)) AS last_name,       -- Trim and try to cast the second attribute to VARCHAR
    TRY_CAST(TRIM($3) AS DATE) AS birth_date,              -- Trim and try to cast the third attribute to DATE
    TRY_CAST(TRIM($4) AS INTEGER) AS age,                  -- Trim and try to cast the fourth attribute to INTEGER
    TRY_CAST(TRIM($5) AS DECIMAL(10,2)) AS salary,         -- Trim and try to cast the fifth attribute to DECIMAL
    TRY_CAST(TRIM($6) AS BOOLEAN) AS is_active,            -- Trim and try to cast the sixth attribute to BOOLEAN
    TRY_CAST(TRIM($7) AS TIMESTAMP) AS hire_date           -- Trim and try to cast the seventh attribute to TIMESTAMP
FROM 
    @my_external_stage
    (FILE_FORMAT => 'my_csv_format');

