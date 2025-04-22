# Medical Device Support Transcripts Project

## Overview

This project demonstrates a complete AI-powered analytics pipeline for customer support conversations in the medical device industry. It showcases how companies can leverage Snowflake's data platform and Cortex AI capabilities to gain valuable insights from support interactions.

## Repository Structure

The repository is organized into four main components representing the end-to-end workflow:

```
Cortex_Transcripts/
├── Create_Transcripts/     # Data generation and initial setup
├── Data_Prep/              # Data transformation and JSON export
├── Analytics_Setup/        # AI-powered analysis using Cortex
└── Streamlit_Apps/         # Interactive visualization dashboards
```

## Project Workflow

### 1. Create_Transcripts

This component establishes the foundation by creating synthetic medical device support data:

- Creates database schema and reference tables for:
  - Support agents
  - Customers
  - Medical devices with categories and common issues
  - Support conversations
- Generates realistic conversation metadata with random timestamps, agent/customer assignments
- Uses Snowflake Cortex to generate AI-powered conversation transcripts based on context

**Key files:**
- `create_cursor_demo_tables.md` - Documentation of database setup
- `create_cursor_demo_tables.sql` - SQL script for data creation
- `CURSOR_DATA_CREATION.ipynb` - Notebook with creation process

### 2. Data_Prep

This component transforms the conversation data and exports it to JSON format:

- Extracts essential conversation data into simplified tables
- Creates procedures for converting data to JSON
- Builds infrastructure for ongoing generation of conversations
- Implements batch processing for automated data generation

**Key files:**
- `support_conv_convert_to_json.md` - Documentation of JSON conversion process
- `support_conv_convert_to_json.sql` - SQL script for data transformation

### 3. Analytics_Setup

This component leverages Snowflake Cortex to analyze conversation content:

- Creates parsing infrastructure for JSON files
- Applies various Cortex LLM functions to analyze transcripts:
  - Summarization
  - Sentiment analysis
  - Device categorization
  - Issue extraction
  - Resolution determination
  - Service quality rating
- Organizes results into dynamic tables that auto-refresh

**Key files:**
- `Cortex_Analysis.md` - Documentation of AI analysis process
- `Cortex_Analysis.sql` - SQL script with Cortex function implementations
- `JSON_to_Table.md` - Documentation of JSON ingestion
- `Create_Dynamic_Tables.md` - Documentation of table creation

### 4. Streamlit_Apps

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

## Technologies Used

- **Snowflake** - Data storage, processing, and Cortex functions
- **SQL** - Database operations and data transformation
- **Python** - Streamlit application development
- **Plotly Express** - Interactive data visualizations
- **Pandas** - Data manipulation
- **Claude/Anthropic AI** - Transcript generation
- **Snowflake Cortex** - AI-powered text analysis

## Getting Started

1. Execute scripts in the `Create_Transcripts` directory to set up the database
2. Run the `Data_Prep` scripts to transform and export the data
3. Execute the `Analytics_Setup` scripts to analyze the conversation content
4. Launch the Streamlit applications to visualize and explore the results

For detailed instructions on each step, refer to the markdown documentation files in each directory.

## Use Cases

This project demonstrates solutions for:

- Analyzing customer sentiment across support interactions
- Identifying common issues for different medical device categories
- Measuring support agent performance with objective metrics
- Tracking resolution rates and service quality
- Creating dashboards for real-time monitoring of support operations

## Documentation

For more comprehensive information about this project, see:
- `Cortex_Transcripts_Project_Overview.md` - Detailed project documentation
- Individual markdown files in each component directory 