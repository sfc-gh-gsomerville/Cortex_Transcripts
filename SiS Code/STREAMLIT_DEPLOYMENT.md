# Support Conversations Analysis - Streamlit in Snowflake Deployment Guide

This guide walks you through deploying the Support Conversations Analysis dashboard as a Streamlit in Snowflake (SiS) application.

## Prerequisites

1. Access to a Snowflake account with the ability to create Streamlit applications
2. The CURSOR_DEMO.V1.SUPPORT_CONVERSATIONS table must exist in your Snowflake account
3. A role with access to the necessary database and warehouse

## Deployment Steps

### 1. Log into Snowflake

Log into your Snowflake account through the web interface (Snowsight).

### 2. Create a Streamlit Application

1. In the left navigation menu, click on **Streamlit Apps**
2. Click the **+ Streamlit App** button in the upper right
3. Fill in the following details:
   - **Name**: Support Conversations Analysis
   - **Database**: CURSOR_DEMO (or the database where your app should be stored)
   - **Schema**: V1 (or the schema where your app should be stored)
   - **Warehouse**: COMPUTE_WH (or any warehouse you have access to)

### 3. Upload the Application Files

1. In the Streamlit app editor, upload the following files:
   - `support_conversations_app.py` (main application file)
   - `requirements_streamlit.txt` (rename to `requirements.txt` when uploading)

2. Alternatively, you can copy and paste the contents of `support_conversations_app.py` into the editor

### 4. Install Required Packages

Make sure the following is in your requirements.txt file:
```
plotly==5.18.0
matplotlib==3.9.0
seaborn==0.13.0
```

### 5. Test the Application

Click the **Run** button in the Snowflake Streamlit editor to make sure your application loads and runs correctly.

### 6. Share the Application

1. Click on the Share button in the upper right
2. Assign roles or specific users who should have access to the application
3. Choose appropriate privileges (Can Use, Can Manage, etc.)
4. Click "Done" to save the sharing settings

## Application Features

The Support Conversations Analysis dashboard includes:

1. **Filters**:
   - Filter by sentiment (positive, neutral, negative)
   - Filter by device type
   - Filter by agent ID
   - Filter by date range

2. **Key Metrics**: View important metrics including total conversations, resolution rate, average duration, and positive sentiment percentage

3. **Visualizations**:
   - Sentiment distribution
   - Issue resolution rate
   - Agent performance analysis
   - Device types breakdown

4. **Conversation Samples**: Search for specific conversations by ID or view random samples

## Troubleshooting

If you encounter any issues:

1. Check that your role has the necessary permissions to access the CURSOR_DEMO.V1.SUPPORT_CONVERSATIONS table
2. Verify the warehouse has enough resources to process the data
3. If charts are not displaying, ensure plotly is properly installed
4. For slow performance, consider optimizing the queries or using a larger warehouse

## Customization

To customize the application:

1. Modify the charts and visualizations in the `support_conversations_app.py` file
2. Add additional filters or metrics as needed
3. Change the color schemes or layout according to your preferences 