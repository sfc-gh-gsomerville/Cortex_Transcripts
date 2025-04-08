# Transcript Analysis Streamlit Apps for Snowflake

This directory contains Streamlit applications designed to visualize and analyze customer support transcript data from the `TRANSCRIPT_ANALYSIS_RESULTS_FINAL` table in Snowflake. These apps leverage Cortex LLM analysis results to provide insights into sentiment, issue resolution, and customer satisfaction.

## Available Applications

1. **transcript_analysis_dashboard.py**: Comprehensive dashboard with multiple tabs for in-depth analysis
   - Overview metrics and visualizations
   - Device category analysis
   - Sentiment analysis
   - Resolution analysis
   - Transcript explorer with filtering

2. **transcript_analysis_basic.py**: Simplified version with core functionality
   - Basic metrics and charts
   - Device category breakdown
   - Simple transcript explorer

## Deploying to Streamlit in Snowflake

To deploy these applications in Snowflake:

1. Sign in to Snowsight (Snowflake's web interface)
2. Navigate to Projects » Streamlit in the left sidebar
3. Click "+ Streamlit App"
4. Enter a name for your app
5. Select the database, schema, and warehouse to use for the app
6. Click "Create"
7. Copy and paste the code from one of the Python files in this directory into the editor
8. Click "Run" to preview the app

## Usage Notes

- The apps assume the existence of a `TRANSCRIPT_ANALYSIS_RESULTS_FINAL` table with the schema as defined in `Cortex_Analysis.sql`
- Ensure your Snowflake role has appropriate access permissions to the table
- The apps automatically refresh data every 5 minutes

## Customization

You can customize these apps by:

- Modifying the SQL queries to include additional data
- Adding new visualizations or analysis tabs
- Adjusting the filters and display options
- Changing the refresh interval for data caching

## Troubleshooting

If you encounter errors:

- Verify that the table `TRANSCRIPT_ANALYSIS_RESULTS_FINAL` exists and is accessible
- Check that your Snowflake warehouse has sufficient resources
- Ensure all required columns are present in the table
- For performance issues, consider adding filters to limit the data volume

For more information on Streamlit in Snowflake, refer to the [official documentation](https://docs.snowflake.com/en/developer-guide/streamlit/getting-started). 