# Medical Device Support Transcript Analysis Project

## Project Overview
This project implements a comprehensive end-to-end data pipeline for analyzing medical device support call transcripts. The system creates synthetic data, generates realistic support conversations, processes them through various stages, and performs advanced analytics using Snowflake's Cortex AI capabilities.

## 1. Initial Data Setup (create_cursor_demo_tables.sql)
The foundation of the project begins with setting up the database structure and populating it with synthetic data:

### Database Structure
- Creates `MED_DEVICE_TRANSCRIPTS` database with `CREATE_TRANSCRIPTS` schema
- Implements four main tables:
  - `SUPPORT_AGENTS`: Stores agent information
  - `CUSTOMERS`: Contains customer details
  - `HOME_MEDICAL_DEVICES`: Comprehensive medical device catalog
  - `SUPPORT_CONVERSATIONS`: Stores conversation metadata and transcripts

### Data Population
- Populates tables with synthetic data:
  - 8 support agents
  - 100 customers
  - 50 medical devices across 10 categories
  - Initial set of support conversations

## 2. Data Preparation Pipeline (support_conv_convert_to_json.sql)
This stage handles the processing and export of conversation data:

### Key Components
- Creates `DATA_PREP` schema for data preparation
- Implements tables for storing conversation data:
  - `support_conv_initial`: Stores initial conversation data
  - `SUPPORT_CONVERSATIONS_NEW`: Handles ongoing conversation data
- Sets up stages for JSON file storage:
  - `call_data_initial`: For initial data
  - `call_data_new`: For ongoing data
- Implements transcript generation procedures using AI

## 3. JSON Processing Pipeline (JSON_to_Table.sql)
This stage handles the ingestion and processing of JSON data:

### Components
- Creates `ANALYTICS` schema
- Implements file format specifications for JSON processing
- Sets up tables for raw JSON data:
  - `RAW_JSON_DATA_INITIAL`: Stores initial JSON data
  - `RAW_JSON_DATA_NEW`: Handles ongoing JSON data
- Implements automated data loading through scheduled tasks

## 4. Dynamic Table Creation (Create_Dynamic_Tables.sql)
This stage creates dynamic tables for real-time data processing:

### Key Features
- Combines data from multiple sources into unified tables
- Creates `combined_raw_json_data` dynamic table
- Implements `parsed_transcripts` dynamic table for structured data
- Provides automatic refresh capabilities
- Includes data validation queries

## 5. Advanced Analytics (Cortex_Analysis.sql)
The final stage implements sophisticated analytics using Snowflake Cortex:

### Analysis Components
1. **Transcript Summarization**
   - Generates concise summaries of support conversations

2. **Sentiment Analysis**
   - Analyzes emotional tone of conversations
   - Categorizes as Positive, Negative, or Neutral

3. **Device Categorization**
   - Classifies conversations into medical device categories
   - Supports 10 different device categories

4. **Issue Analysis**
   - Extracts main issues from conversations
   - Determines resolution status
   - Provides confidence scores

5. **Customer Service Rating**
   - Rates service quality on a 0-10 scale
   - Provides justification for ratings

## Technical Implementation Details
- Uses Snowflake's native features:
  - Dynamic tables for real-time processing
  - Scheduled tasks for automation
  - File formats for data ingestion
  - Stages for file storage
- Leverages Snowflake Cortex AI capabilities:
  - LLM functions for text analysis
  - Sentiment analysis
  - Text classification
  - Question answering
  - Text completion

## Data Flow
1. Initial data creation and population
2. Conversation data generation and JSON export
3. JSON data ingestion and processing
4. Dynamic table creation and real-time updates
5. Advanced analytics using Cortex AI

## Key Features
- End-to-end automation
- Real-time data processing
- Advanced AI-powered analytics
- Scalable architecture
- Comprehensive medical device support analysis 