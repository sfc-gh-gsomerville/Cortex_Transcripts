# Analytics Setup Description

## Overview
This folder contains the setup for loading and processing JSON data from the Call Data Stage into a Snowflake table for analytics purposes.

## Components

### JSON_to_Table.sql
This SQL file contains the setup for automatically loading JSON files from the Call Data Stage into a Snowflake table.

#### Key Features:
- Creates an ANALYTICS schema in the Cursor_Demo database
- Defines a JSON file format for handling compressed JSON files
- Creates a table (RAW_JSON_DATA) to store the loaded JSON data with:
  - FILE_NAME: Name of the source file
  - FILE_LOAD_TIME: Timestamp when the file was loaded
  - JSON_DATA: The actual JSON content as a VARIANT type

#### Task Configuration:
- Task Name: LOAD_JSON_FILES
- Warehouse: CORTEX_DEMO_WH
- Schedule: Runs every 15 seconds
- Function: Automatically loads new JSON files from the Call Data Stage

#### File Format Settings:
- Type: JSON
- Strip Outer Array: TRUE
- Replace Invalid Characters: TRUE
- Date/Time/Timestamp Format: AUTO

## Usage
1. The task automatically runs every 15 seconds
2. It checks for new JSON files in the Call Data Stage
3. New files are loaded into the RAW_JSON_DATA table
4. Each record includes the source file name and load timestamp

## Task Management
- To suspend the task: `ALTER TASK Cursor_Demo.ANALYTICS.LOAD_JSON_FILES SUSPEND;`
- To resume the task: `ALTER TASK Cursor_Demo.ANALYTICS.LOAD_JSON_FILES RESUME;` 