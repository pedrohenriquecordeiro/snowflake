-- In Snowflake, MERGE is a DML (Data Manipulation Language) command that allows 
-- you to combine data from a source (such as a staging table or external file) into a target table. 
-- It performs insert, update, or delete operations based on specified conditions, 
-- making it useful for synchronizing or upserting data (updating existing records and inserting new ones).


MERGE INTO target_table AS target                   -- Target table where data will be merged
USING source_table AS source                        -- Source table containing the new or updated data
ON target.id = source.id                            -- Condition to match records based on the `id` field

-- If a match is found, update the existing record in the target table
WHEN MATCHED THEN
    UPDATE SET
        target.column1 = source.column1,            -- Update column1
        target.column2 = source.column2,            -- Update column2
        target.column3 = source.column3             -- Update column3

-- If no match is found, insert a new record into the target table
WHEN NOT MATCHED THEN
    INSERT (id, column1, column2, column3)
    VALUES (source.id, source.column1, source.column2, source.column3);


-- source is a statement select
MERGE INTO target_table AS target                        -- Target table where data will be merged
USING (
    -- The SELECT statement as the source of data
    SELECT 
        id, 
        column1, 
        column2, 
        column3
    FROM source_table
    WHERE column3 > 100                                  -- Example filter in the source data
) AS source                                              -- Alias for the derived source data
ON target.id = source.id                                 -- Condition to match records based on the `id` field

-- If a match is found, update the existing record in the target table
WHEN MATCHED THEN
    UPDATE SET
        target.column1 = source.column1,                 -- Update column1
        target.column2 = source.column2,                 -- Update column2
        target.column3 = source.column3                  -- Update column3

-- If no match is found, insert a new record into the target table
WHEN NOT MATCHED THEN
    INSERT (id, column1, column2, column3)
    VALUES (source.id, source.column1, source.column2, source.column3);
