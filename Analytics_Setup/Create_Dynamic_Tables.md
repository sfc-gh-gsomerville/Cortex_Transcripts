# Create Dynamic Tables Documentation

## Overview
This document provides details about the `Create_Dynamic_Tables.sql` script, which is designed to create and manage dynamic tables in the Snowflake analytics environment. The script combines raw JSON data from multiple sources, creates a unified dynamic table, and parses JSON transcript data into structured fields for easier analysis.

## Script Components

### 1. Raw Data Exploration
```sql
-- Show top 10 records from RAW_JSON_DATA_INITIAL
SELECT TOP 10 *
FROM RAW_JSON_DATA_INITIAL
ORDER BY 2;

-- Show top 10 records from RAW_JSON_DATA_NEW
SELECT TOP 10 *
FROM RAW_JSON_DATA_NEW
ORDER BY 2;
```
These initial statements retrieve sample data from the two source tables (`RAW_JSON_DATA_INITIAL` and `RAW_JSON_DATA_NEW`) to provide a quick view of the data structure and content.

### 2. Combined Raw JSON Dynamic Table
```sql
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
```
This section creates a dynamic table that combines data from both source tables using a UNION ALL operation. A new `source` column is added to identify which source table each record comes from. The table is configured with:
- `TARGET_LAG = 'DOWNSTREAM'`: Updates when source tables are updated
- `WAREHOUSE = CORTEX_DEMO_WH`: Specifies the Snowflake warehouse to use
- `REFRESH_MODE = 'AUTO'`: Sets automatic refreshing

### 3. Sample Combined Data Query
```sql
-- Show records from the new dynamic table
SELECT * 
FROM combined_raw_json_data
ORDER BY source, 2
LIMIT 20;
```
This query returns 20 sample records from the combined dynamic table, ordered by source and the second column, to verify the data combination process.

### 4. JSON Data Parsing Example
```sql
-- Parse transcript JSON data using dot notation
SELECT
  source,
  JSON_DATA:agent_name::STRING as agent_name,
  JSON_DATA:customer_name::STRING as customer_name,
  JSON_DATA:conversation_id::NUMBER as conversation_id,
  JSON_DATA:start_time::TIMESTAMP_NTZ as start_time,
  JSON_DATA:end_time::TIMESTAMP_NTZ as end_time,
  JSON_DATA:transcript::STRING as transcript
FROM combined_raw_json_data
LIMIT 50;
```
This statement demonstrates how to extract structured data from the JSON using Snowflake's dot notation, parsing specific fields into typed columns.

### 5. Parsed Transcripts Dynamic Table
```sql
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
```
This creates a dynamic table that automatically parses the JSON data into a structured format with typed columns. Like the previous dynamic table, it uses the CORTEX_DEMO_WH warehouse and automatic refreshing.

### 6. Sample Parsed Data Query
```sql
-- Show 10 records from the parsed_transcripts dynamic table
SELECT
  source,
  conversation_id,
  start_time,
  end_time,
  agent_name,
  customer_name,
  SUBSTRING(transcript, 1, 100) as transcript_preview
FROM parsed_transcripts
LIMIT 10;
```
This final query samples 10 records from the parsed dynamic table, showing how the structured data can be easily queried. It includes a preview of the transcript field (first 100 characters) for readability.

## Usage
This script should be executed in a Snowflake environment with appropriate permissions to create dynamic tables and access the raw data sources. The CORTEX_DEMO_WH warehouse must exist and be accessible to the user running the script. 