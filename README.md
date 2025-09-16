# Cortex Transcripts Project Overview

## Project Summary
This project demonstrates a complete end-to-end data pipeline for processing, analyzing, and visualizing customer support conversations for a medical device company. The system creates synthetic data, generates realistic conversation transcripts using AI, processes the data through various stages, and provides analytical insights using Snowflake's Cortex AI capabilities. The project showcases advanced Snowflake features including database management, stored procedures, dynamic tables, AI-powered analysis, and data visualization.

## Getting Started

1. In Snowflake, copy the SQL script in the file `Account_Setup.sql` and execute the script
   (Note:  After running this script you will be able to create new workspce from this GIT repository and executed the following SQL scripts from directly within Snowflake Workspaces
2. In the "Create_Transcripts" folder, execute the script `create_transcripts_demo_table.sql`
3. In the "Data_Prep" folder, execute the script `create_transcripts_demo_table.sql`
4. In the "Analytics_Setup" folder, execute the script `JSON_to_Table.sql`
5. In the "Analytics_Setup" folder, execute the script `Create_Dynamic_Tables.sql`
6. In the "Analytics_Setup" folder, execute the script `Cortex_Analysis.sql`
7. In the "Streamlit_Apps" folder, copy the python code in `Med_Device_Transcripts_Overview.py` into a Streamlit-in-Snoflake (SiS) app in Snowflake to visualize and explore the results
8. If you would like to review AISQL and Cortex LLM functions, as well as the creation of Dynamic Tables in a Snowflake Notebook, be sure to download the `MED_TECH_TRANSCRIPTS_CORTEX_ANALYSIS_AISQL.ipynb` file from the "Analytics_Setup/MED_TRANSCRIPTIONS_CORTEX_ANALYSIS" folder to your local machine and then open a new notebook in Snowflake from a .jpynb file!

For detailed instructions on each step, refer to the markdown documentation files in each directory.

## 1. Data Creation and Initial Setup (create_transactions_demo_tables.md)

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

**Key files:**
- `create_transcripts_demo_tables.md` - Documentation of database setup
- `create_transcripts_demo_tables.sql` - SQL script for data creation
- `transcripts_DATA_CREATION.ipynb` - Notebook with creation process

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
 
**Key files:**
- `support_conv_convert_to_json.md` - Documentation of JSON conversion process
- `support_conv_convert_to_json.sql` - SQL script for data transformation

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
 
**Key files:**
- `JSON_to_Table.md` - Documentation of JSON ingestion
- `JOSN_to_Table.sql` - SQL Script for JSON ingestion

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
 
**Key files:**
- `Create_Dynamic_Tables.md` - Documentation of table creation
- `Create_Dynamis_Tables.sql` - SQL script for creating initial dynamic tables

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
 
**Key files:**
- `Cortex_Analysis.md` - Documentation of AI analysis process
- `Cortex_Analysis.sql` - SQL script with Cortex function implementations

## 6. Streamlit_Apps

This component provides interactive web applications to visualize the analyzed data:

- **Med_Device_Transcripts_Overview**: Main dashboard with three tabs:
  - Overview Dashboard - Key metrics and distributions
  - Agent Metrics - Performance analysis for individual agents
  - Record Viewer - Detailed exploration of transcripts
- Interactive filtering by date, agent, sentiment, device category, etc.
- Comprehensive visualizations using Plotly Express

**Key files:**
- `Med_Device_Transcripts_Overview.py` - Main Streamlit application
- `Med_Device_Transcript_Overview_Description.md` - Detailed documentation
- `transcript_analysis_dashboard.py` - Additional dashboard

## Project Architecture and Data Flow

The complete project follows this data flow:

1. **Data Creation**: Generate synthetic support conversation data with realistic attributes
2. **Transcript Generation**: Create AI-powered conversation transcripts using Claude
3. **JSON Export**: Export conversation data to JSON format with timestamps
4. **Data Ingestion**: Continuously load JSON files into Snowflake tables
5. **Data Transformation**: Parse and structure the JSON data into clean tables
6. **AI Analysis**: Apply various Cortex AI functions to extract insights
7. **Results Consolidation**: Combine all analyses into comprehensive result tables
8. **Streamlit-in-Snowflake**:  Interactive visualization dashboards

## Key Technical Features

- **Snowflake Cortex AI Integration**: Leverages Claude and other AI models for content generation and analysis
- **Stored Procedures**: Implements complex logic in SQL and JavaScript procedures
- **Dynamic Tables**: Creates self-refreshing tables that automatically update with new data
- **Scheduled Tasks**: Automates data ingestion with configurable timing
- **JSON Processing**: Handles semi-structured data with Snowflake's VARIANT type
- **Foreign Key Relationships**: Maintains data integrity across related tables
- **Batch Processing**: Supports efficient processing of multiple records
- **Python**: Streamlit application development using Pandas and Plotly Express

## Use Cases

This project demonstrates solutions for:

- Analyzing customer sentiment across support interactions
- Identifying common issues for different medical device categories
- Measuring support agent performance with objective metrics
- Tracking resolution rates and service quality
- Creating dashboards for real-time monitoring of support operations

## Business Value

This project demonstrates how to:

- Create realistic synthetic data for testing and demonstration
- Process and analyze customer support conversations at scale
- Extract meaningful insights from unstructured conversation data
- Automate the entire pipeline from data creation to analysis
- Provide actionable insights for improving customer support operations

The system can be used to identify common issues, measure agent performance, track resolution rates, and understand customer sentiment across different medical device categories. 
