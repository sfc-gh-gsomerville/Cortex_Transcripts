# Support Conversations Analysis - Streamlit in Snowflake

This folder contains all the necessary files for deploying a Streamlit in Snowflake (SiS) application that analyzes customer support conversations data.

## Folder Contents

- `support_conversations_app.py` - Full-featured Streamlit application with Plotly visualizations
- `support_conversations_app_simplified.py` - Simplified version using only built-in Streamlit visualizations
- `requirements_streamlit.txt` - Required packages for the full-featured version
- `STREAMLIT_DEPLOYMENT.md` - Detailed deployment guide for Snowflake

## Application Features

The Support Conversations Analysis dashboard provides:

1. **Interactive Filtering**: Filter conversations by sentiment, device type, agent ID, and date range
2. **Key Metrics**: View total conversations, resolution rate, average duration, and sentiment distribution
3. **Visual Analysis**: Charts and tables showing sentiment and resolution patterns
4. **Agent Performance**: Analysis of agent effectiveness and resolution rates
5. **Device Analysis**: Breakdown of support issues by device type
6. **Conversation Explorer**: Search and view individual conversation transcripts

## Application Versions

The project includes two versions of the Streamlit application:

### 1. Full-Featured Version (`support_conversations_app.py`)

- Uses Plotly for advanced, interactive visualizations
- Provides more sophisticated charts and data displays
- Requires additional packages (plotly, matplotlib, seaborn)

### 2. Simplified Version (`support_conversations_app_simplified.py`)

- Uses only built-in Streamlit visualizations
- Doesn't require additional packages beyond what's included in Snowflake
- More likely to deploy without package installation issues
- Provides the same analysis capabilities with simpler visuals

## Deployment Instructions

Please refer to the `STREAMLIT_DEPLOYMENT.md` file for detailed steps on how to deploy either version of the application to Snowflake.

### Quick Start

1. Log in to your Snowflake account
2. Create a new Streamlit app 
3. Copy the contents of either application file into the editor
4. If using the full-featured version, ensure the required packages are installed
5. Run the application

## Prerequisites

- Access to a Snowflake account with Streamlit in Snowflake capability
- The CURSOR_DEMO.V1.SUPPORT_CONVERSATIONS table must exist in your Snowflake account
- Appropriate permissions for the database and warehouse

## Data Overview

The application analyzes the following data from support conversations:

- Sentiment (positive, neutral, negative)
- Issue resolution status
- Device types
- Agent performance
- Conversation duration
- Common issues

## Customization

Both applications can be customized to match your specific needs:

- Change the table name if your data is stored differently
- Modify the filters, charts, and metrics
- Adjust the visual appearance and layout

## Troubleshooting

If you encounter issues with the full-featured version, try the simplified version first to ensure basic functionality. For detailed troubleshooting guidance, refer to the deployment guide.
