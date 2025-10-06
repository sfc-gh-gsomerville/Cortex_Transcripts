-- Create_Dynamic_Tables.sql
-- SQL script for creating dynamic tables in the analytics setup

-- Set Context to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Set worksheet Context
USE DATABASE MED_DEVICE_TRANSCRIPTS;
USE SCHEMA ANALYTICS;

-- Show top 10 records from RAW_JSON_DATA_INITIAL
SELECT TOP 10 *
FROM RAW_JSON_DATA_INITIAL
ORDER BY 2;

-- Show top 10 records from RAW_JSON_DATA_NEW
SELECT TOP 10 *
FROM RAW_JSON_DATA_NEW
ORDER BY 2;

-- Create dynamic table that combines RAW_JSON_DATA_INITIAL and RAW_JSON_DATA_NEW
CREATE OR REPLACE DYNAMIC TABLE combined_raw_json_data
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CORTEX_DEMO_WH
  REFRESH_MODE = 'AUTO'
AS
    SELECT 
        'INITIAL' as source,
        *
        FROM RAW_JSON_DATA_INITIAL
        UNION ALL
        SELECT 
        'NEW' as source,
        *
    FROM RAW_JSON_DATA_NEW;

-- Show records from the new dynamic table
SELECT * 
FROM combined_raw_json_data
LIMIT 20;

-- Parse transcript JSON data using dot notation
SELECT
  source,
  JSON_DATA:agent_name::STRING as agent_name,
  JSON_DATA:customer_name::STRING as customer_name,
  JSON_DATA:conversation_id::NUMBER as conversation_id,
  JSON_DATA:start_time::TIMESTAMP_NTZ as start_time,
  JSON_DATA:end_time::TIMESTAMP_NTZ as end_time,
  --DATEDIFF('minute', JSON_DATA:start_time::TIMESTAMP_NTZ, JSON_DATA:end_time::TIMESTAMP_NTZ) as conversation_duration_minutes,
  JSON_DATA:transcript::STRING as transcript
FROM combined_raw_json_data
LIMIT 50;

-- Create a parsed transcript dynamic table
CREATE OR REPLACE DYNAMIC TABLE parsed_transcripts
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CORTEX_DEMO_WH
  REFRESH_MODE = 'AUTO'
AS
SELECT
  source,
  JSON_DATA:conversation_id::NUMBER as conversation_id,
  JSON_DATA:start_time::TIMESTAMP_NTZ as start_time,
  JSON_DATA:end_time::TIMESTAMP_NTZ as end_time,
  JSON_DATA:agent_name::STRING as agent_name,
  JSON_DATA:customer_name::STRING as customer_name,
  JSON_DATA:transcript::STRING as transcript
FROM combined_raw_json_data;

-- Show 10 records from the parsed_transcripts dynamic table
SELECT
  source,
  conversation_id,
  start_time,
  end_time,
  agent_name,
  customer_name,
  transcript
FROM parsed_transcripts
LIMIT 10;

