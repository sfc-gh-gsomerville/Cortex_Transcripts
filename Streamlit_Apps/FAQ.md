# Transcript Analysis Streamlit Apps - FAQ

## General Questions

### What data is being analyzed in these apps?
These apps analyze customer support transcript data that has been processed with Snowflake Cortex LLM functions for sentiment analysis, issue identification, resolution status, and customer satisfaction rating.

### How often is the data refreshed?
The apps cache data for 5 minutes (300 seconds) before refreshing. You can manually refresh by clicking the "Run" button in Snowsight.

### Can I export data from the apps?
Yes, many of the charts have export options in the top-right corner. For tables, you can hover over them and click the download button that appears.

## Technical Questions

### What permissions do I need to run these apps?
You need:
- READ access to the `TRANSCRIPT_ANALYSIS_RESULTS_FINAL` table
- USAGE permission on the warehouse specified for the app
- Appropriate permissions on the database and schema where the app is deployed

### Why am I getting a "Table not found" error?
This can happen if:
- The table does not exist in the database/schema specified when creating the app
- Your role does not have access to the table
- You're using a different table name than expected in the app's code

### How can I optimize performance for large datasets?
- Use a larger warehouse for the app
- Add more specific filters to limit the data processed
- Implement additional caching strategies in the code
- Consider creating materialized views or aggregation tables

### Can I connect to multiple Snowflake tables?
Yes, you can modify the code to query multiple tables and join the results. Update the load_data() function to include additional queries.

## Customization Questions

### How do I add a new visualization?
1. Determine which tab the visualization should appear in
2. Add the appropriate code using Plotly or Streamlit's native visualization tools
3. Ensure your data is formatted correctly for the visualization
4. Test thoroughly before deploying

### Can I modify the filters?
Yes, the sidebar filters can be customized by editing the filter section in the code. You can:
- Add new filters based on additional columns
- Change the default selections
- Modify the filter UI components (e.g., dropdown vs. multiselect)

### How do I add a new analysis tab?
1. In the tabs definition, add a new tab name to the list
2. Create a new `with tab#:` section with your content
3. Add your visualizations and analysis within this section

## Deployment Questions

### Can I schedule automatic updates to the app?
Streamlit in Snowflake doesn't directly support scheduling, but:
- The data itself can be updated on a schedule using Snowflake tasks
- The app will reflect the latest data when refreshed (either automatically after cache expiry or manually)

### Can I share the app with others?
Yes, once deployed in Snowflake, you can share the app with other users by:
1. Granting appropriate permissions on the app object
2. Sharing the app URL with authorized users

### Is there a limit to how many apps I can deploy?
Snowflake may have account-specific limits. Check with your Snowflake administrator for details about your account's limitations. 