# Data Preparation for Support Conversations

This directory contains scripts for preparing and processing customer support conversation data.

## Overview of `support_conv_convert_to_json.sql`

The `support_conv_convert_to_json.sql` script creates a data processing pipeline that:

1. Creates a schema for data preparation
2. Sets up storage infrastructure (stage and file format)
3. Creates tables for conversation data
4. Generates realistic support conversation transcripts using Claude AI
5. Exports the conversations to JSON files with timestamps in the filenames
6. Provides a complete end-to-end procedure for processing new conversations

## Components

### Schema and Storage

- **Schema**: Creates `Cursor_Demo.DATA_PREP` schema
- **File Format**: Defines JSON file format for data export
- **Stage**: Sets up `call_data_stage` internal stage for storing JSON files

### Tables

- **SUPPORT_CONVERSATIONS_ADD_RECORDS**: Primary table that stores conversation details including:
  - 4+ digit unique conversation IDs
  - Timestamps (start/end)
  - Agent and customer information
  - Sentiment analysis (positive/negative/neutral)
  - Resolution status
  - Device details and issue information
  - Generated transcript content

- **support_conv_initial**: Simplified table containing only the essential conversation fields:
  - Conversation ID
  - Start/end times
  - Agent and customer names
  - Transcript

### Procedures

1. **GENERATE_TRANSCRIPTS_ADD_RECORDS**:
   - Finds records without transcripts
   - Constructs detailed prompts for Claude AI based on conversation context
   - Generates realistic support conversation transcripts
   - Updates records with generated content

2. **EXPORT_CONVERSATIONS_TO_JSON**:
   - Creates a timestamp-based filename (format: YYYYMMDD_HHMMSS)
   - Structures data as JSON objects with selected fields
   - Exports consolidated data to a JSON file in the stage
   - Returns success information with the filename

3. **PROCESS_NEW_CONVERSATION**:
   - Orchestrates the complete conversation processing pipeline:
   - Generates a unique 4+ digit conversation ID
   - Creates a new random conversation record
   - Calls transcript generation procedure
   - Loads data into the simplified table
   - Exports to JSON
   - Truncates tables to prepare for the next run
   - Returns status information for the complete process

## Usage

To process a new customer support conversation:

```sql
CALL Cursor_Demo.DATA_PREP.PROCESS_NEW_CONVERSATION();
```

This will generate a complete conversation with a random customer, agent, and medical device issue, generate a realistic transcript using Claude AI, export it to a timestamped JSON file, and clean up the tables afterward.

The resulting JSON files are stored in the `Cursor_Demo.DATA_PREP.call_data_stage` and contain the conversation ID, timestamps, agent and customer names, and the full conversation transcript. 
