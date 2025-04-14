# Cortex Transcripts Project Overview

## Project Summary
This project demonstrates a complete end-to-end data pipeline for processing, analyzing, and visualizing customer support conversations for a medical device company. The system creates synthetic data, generates realistic conversation transcripts using AI, processes the data through various stages, and provides analytical insights using Snowflake's Cortex AI capabilities. The project showcases advanced Snowflake features including database management, stored procedures, dynamic tables, AI-powered analysis, and data visualization.

## 1. Data Creation and Initial Setup (create_cursor_demo_tables.md)

The foundation of the project begins with creating a comprehensive demo database for a medical device support system:

- **Database Structure**: Creates a `MED_DEVICE_TRANSCRIPTS` database with the `CREATE_TRANSCRIPTS` schema to organize all demo data objects
- **Core Tables**:
  - `SUPPORT_AGENTS`: Stores information about customer support representatives
  - `CUSTOMERS`: Contains customer information
  - `HOME_MEDICAL_DEVICES`: Comprehensive catalog of medical devices with detailed attributes
  - `SUPPORT_CONVERSATIONS`: Stores conversation metadata and transcripts

- **Data Population**:
  - Populates tables with realistic sample data
  - Creates 8 support agents, 100 customers, and 50 medical devices across various categories
  - Generates 5 random support conversations with realistic attributes

- **AI-Powered Transcript Generation**:
  - Creates a stored procedure that uses Claude AI via Snowflake Cortex
  - Generates realistic conversation transcripts based on context (device type, issue, sentiment)
  - Updates conversation records with the generated transcripts

- **Data Export**:
  - Creates a simplified table in the Data_Prep schema
  - Exports conversation data to JSON format for use in demonstrations

## 2. Support Conversation Processing (support_conv_convert_to_json.md)

This component establishes a data processing pipeline for handling support conversations:

- **Schema Creation**: Sets up a `DATA_PREP` schema for data preparation tasks
- **Initial Data Table**: Creates a `support_conv_initial` table using a CTAS statement
- **Stage Creation**: Establishes a stage for storing JSON files
- **JSON Export**: Exports conversation data to JSON format with proper structure
- **New Conversation Processing**:
  - Creates a table for new support conversations
  - Develops a procedure to generate transcripts for new records
  - Implements a procedure to export conversations to timestamped JSON files
  - Provides a batch processing procedure to handle multiple conversations

- **Pipeline Automation**:
  - Orchestrates the entire process from data creation to JSON export
  - Includes error handling and status reporting
  - Supports both single conversation and batch processing modes

## 3. JSON Data Ingestion (JSON_to_Table.md)

This component handles the ingestion of JSON data into Snowflake tables:

- **Schema Setup**: Creates an `ANALYTICS` schema for data analysis
- **File Format Definition**: Establishes a JSON file format specification
- **Data Tables**:
  - Creates tables for both initial and new JSON data
  - Stores file metadata and the JSON content as VARIANT type

- **Automated Ingestion**:
  - Implements a scheduled task that runs every 15 seconds
  - Continuously loads new JSON files from the stage into tables
  - Provides task control commands (resume/suspend)

- **Orchestration**:
  - Creates a stored procedure to manage the entire pipeline
  - Coordinates task execution, batch processing, and timing
  - Ensures proper sequencing of operations with appropriate delays

## 4. Dynamic Table Creation (Create_Dynamic_Tables.md)

This component transforms the raw JSON data into structured tables for analysis:

- **Data Verification**: Queries the raw JSON tables to verify data ingestion
- **Combined Data Table**: Creates a dynamic table that unifies initial and new data
- **Parsed Transcript Table**: 
  - Extracts structured fields from the JSON data
  - Creates a clean table with conversation metadata and transcripts
  - Uses dot notation to navigate the JSON structure

- **Dynamic Table Benefits**:
  - Automatically refreshes as new data arrives
  - Provides a consistent view of all conversation data
  - Simplifies downstream analysis by presenting clean, structured data

## 5. Cortex AI Analysis (Cortex_Analysis.md)

This component leverages Snowflake's Cortex AI capabilities to analyze the conversation data:

- **Transcript Summarization**: Uses `SNOWFLAKE.CORTEX.SUMMARIZE` to create concise summaries
- **Sentiment Analysis**: 
  - Analyzes sentiment using `SNOWFLAKE.CORTEX.SENTIMENT`
  - Categorizes conversations as positive, negative, or neutral

- **Device Classification**:
  - Classifies conversations into medical device categories
  - Uses `SNOWFLAKE.CORTEX.CLASSIFY_TEXT` with predefined categories

- **Issue Extraction**:
  - Identifies the main issue from each transcript
  - Uses `SNOWFLAKE.CORTEX.EXTRACT_ANSWER` with specific prompting

- **Resolution Analysis**:
  - Determines if issues were resolved
  - Uses `SNOWFLAKE.CORTEX.COMPLETE` with a system prompt for consistent analysis

- **Customer Service Rating**:
  - Rates the customer service experience on a scale of 0-10
  - Provides reasoning for each rating

- **Dynamic Analysis Tables**:
  - Creates dynamic tables for various analysis components
  - Combines all analyses into a comprehensive results table
  - Processes JSON fields to extract structured information

## Project Architecture and Data Flow

The complete project follows this data flow:

1. **Data Creation**: Generate synthetic support conversation data with realistic attributes
2. **Transcript Generation**: Create AI-powered conversation transcripts using Claude
3. **JSON Export**: Export conversation data to JSON format with timestamps
4. **Data Ingestion**: Continuously load JSON files into Snowflake tables
5. **Data Transformation**: Parse and structure the JSON data into clean tables
6. **AI Analysis**: Apply various Cortex AI functions to extract insights
7. **Results Consolidation**: Combine all analyses into comprehensive result tables

## Key Technical Features

- **Snowflake Cortex AI Integration**: Leverages Claude and other AI models for content generation and analysis
- **Stored Procedures**: Implements complex logic in SQL and JavaScript procedures
- **Dynamic Tables**: Creates self-refreshing tables that automatically update with new data
- **Scheduled Tasks**: Automates data ingestion with configurable timing
- **JSON Processing**: Handles semi-structured data with Snowflake's VARIANT type
- **Foreign Key Relationships**: Maintains data integrity across related tables
- **Batch Processing**: Supports efficient processing of multiple records

## Business Value

This project demonstrates how to:

- Create realistic synthetic data for testing and demonstration
- Process and analyze customer support conversations at scale
- Extract meaningful insights from unstructured conversation data
- Automate the entire pipeline from data creation to analysis
- Provide actionable insights for improving customer support operations

The system can be used to identify common issues, measure agent performance, track resolution rates, and understand customer sentiment across different medical device categories. 
