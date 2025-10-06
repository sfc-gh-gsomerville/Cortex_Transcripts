-- Set Context to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create ANALYTICS schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS MED_DEVICE_TRANSCRIPTS.ANALYTICS;

-- Create a file format for compressed JSON files
CREATE OR REPLACE FILE FORMAT MED_DEVICE_TRANSCRIPTS.ANALYTICS.JSON_GZ_FORMAT
	TYPE=JSON
    STRIP_OUTER_ARRAY=TRUE
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT=AUTO
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO;

-- Create a warehouse for Cortex operations
CREATE OR REPLACE WAREHOUSE CORTEX_DEMO_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE;

--Create a table to store the JSON files created the initial data creation
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.ANALYTICS.RAW_JSON_DATA_INITIAL (
    FILE_NAME VARCHAR,
    FILE_LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    JSON_DATA VARIANT
);

-- Copy the initial JSON file into the a table
COPY INTO "MED_DEVICE_TRANSCRIPTS"."ANALYTICS"."RAW_JSON_DATA_INITIAL" 
FROM (SELECT 
    METADATA$FILENAME,
    CURRENT_TIMESTAMP(),
    $1::VARIANT
    FROM '@"MED_DEVICE_TRANSCRIPTS"."DATA_PREP"."CALL_DATA_INITIAL"') 
FILE_FORMAT = '"MED_DEVICE_TRANSCRIPTS"."ANALYTICS"."JSON_GZ_FORMAT"' 
ON_ERROR=ABORT_STATEMENT 
 ;


--Create a table to store the new JSON files that are being created in the LIVE data flow
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.ANALYTICS.RAW_JSON_DATA_NEW (
    FILE_NAME VARCHAR,
    FILE_LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    JSON_DATA VARIANT
);

-- Create a task to load new JSON files every 15 seconds
CREATE OR REPLACE TASK MED_DEVICE_TRANSCRIPTS.ANALYTICS.LOAD_JSON_FILES_NEW
    WAREHOUSE = CORTEX_DEMO_WH
    SCHEDULE = '15 seconds'
AS
    COPY INTO "MED_DEVICE_TRANSCRIPTS"."ANALYTICS"."RAW_JSON_DATA_NEW" 
    FROM (SELECT 
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        $1::VARIANT
        FROM '@"MED_DEVICE_TRANSCRIPTS"."DATA_PREP"."CALL_DATA_NEW"') 
    FILE_FORMAT = '"MED_DEVICE_TRANSCRIPTS"."ANALYTICS"."JSON_GZ_FORMAT"' 
    ON_ERROR=ABORT_STATEMENT 
 ;


-- Resume the task to start processing
ALTER TASK MED_DEVICE_TRANSCRIPTS.ANALYTICS.LOAD_JSON_FILES_NEW RESUME;

-- Suspend the task
ALTER TASK MED_DEVICE_TRANSCRIPTS.ANALYTICS.LOAD_JSON_FILES_NEW SUSPEND;

-- Call the new procedure to process conversations (default 3 times)
--CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.PROCESS_CONVERSATIONS_BATCH();

-- Example with explicit execution count
-- CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.PROCESS_CONVERSATIONS_BATCH(5);

-- Create a stored procedure to manage the task and batch processing
CREATE OR REPLACE PROCEDURE MED_DEVICE_TRANSCRIPTS.ANALYTICS.RUN_NEW_TRANSCRIPT_PIPELINE()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
    try {
        // Resume the task
        var resume_stmt = snowflake.createStatement({
            sqlText: "ALTER TASK MED_DEVICE_TRANSCRIPTS.ANALYTICS.LOAD_JSON_FILES_NEW RESUME;"
        });
        resume_stmt.execute();
        
        // Call the batch processing with 3 iterations
        var batch_stmt = snowflake.createStatement({
            sqlText: "CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.PROCESS_CONVERSATIONS_BATCH(3);"
        });
        batch_stmt.execute();
        
        // Add a 16 second delay using Snowflake's system$wait function so that the last file created is loaded into the table
        var wait_stmt = snowflake.createStatement({
            sqlText: "CALL system$wait(16);"
        });
        wait_stmt.execute();
        
        // Suspend the task after processing completes and delay
        var suspend_stmt = snowflake.createStatement({
            sqlText: "ALTER TASK MED_DEVICE_TRANSCRIPTS.ANALYTICS.LOAD_JSON_FILES_NEW SUSPEND;"
        });
        suspend_stmt.execute();
        
        return "Batch processing completed successfully: Task resumed, 3 batches processed, waited 16 seconds, then task suspended.";
    } catch (err) {
        return "Error during batch processing: " + err;
    }
$$;

-- Example of how to run the procedure
CALL MED_DEVICE_TRANSCRIPTS.ANALYTICS.RUN_NEW_TRANSCRIPT_PIPELINE();
