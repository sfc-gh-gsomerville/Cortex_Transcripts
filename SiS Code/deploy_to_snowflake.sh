#!/bin/bash
# Script to help deploy Streamlit in Snowflake app using SnowSQL
# Note: You need to have SnowSQL installed and configured to use this script

# Configuration variables - modify these with your Snowflake details
SNOWFLAKE_ACCOUNT="your_account_id" # e.g., xy12345.us-east-1.aws
SNOWFLAKE_USER="your_username"
SNOWFLAKE_ROLE="your_role" # e.g., ACCOUNTADMIN
SNOWFLAKE_DATABASE="CURSOR_DEMO"
SNOWFLAKE_SCHEMA="V1"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
SNOWFLAKE_STAGE="${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.STREAMLIT_STAGE"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if SnowSQL is installed
if ! command -v snowsql &> /dev/null; then
    echo -e "${RED}Error: SnowSQL is not installed or not in your PATH.${NC}"
    echo "Please install SnowSQL first: https://docs.snowflake.com/en/user-guide/snowsql-install-config"
    exit 1
fi

echo -e "${YELLOW}Deploying Streamlit app to Snowflake...${NC}"

# Create the stage if it doesn't exist
echo -e "${YELLOW}Creating stage if it doesn't exist...${NC}"
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -d $SNOWFLAKE_DATABASE -s $SNOWFLAKE_SCHEMA -w $SNOWFLAKE_WAREHOUSE -q "CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE"

# Choose which version to deploy
echo -e "${YELLOW}Which version would you like to deploy?${NC}"
echo "1) Full version with Plotly (support_conversations_app.py)"
echo "2) Simplified version (support_conversations_app_simplified.py)"
read -p "Enter choice [1-2]: " VERSION_CHOICE

if [ "$VERSION_CHOICE" == "1" ]; then
    APP_FILE="support_conversations_app.py"
    echo -e "${YELLOW}Uploading full version with dependencies...${NC}"
    # Upload requirements.txt
    snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -d $SNOWFLAKE_DATABASE -s $SNOWFLAKE_SCHEMA -w $SNOWFLAKE_WAREHOUSE -q "PUT file://$(pwd)/requirements_streamlit.txt @$SNOWFLAKE_STAGE/requirements.txt OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
elif [ "$VERSION_CHOICE" == "2" ]; then
    APP_FILE="support_conversations_app_simplified.py"
    echo -e "${YELLOW}Uploading simplified version...${NC}"
else
    echo -e "${RED}Invalid choice. Exiting.${NC}"
    exit 1
fi

# Upload the selected app file
echo -e "${YELLOW}Uploading $APP_FILE...${NC}"
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -d $SNOWFLAKE_DATABASE -s $SNOWFLAKE_SCHEMA -w $SNOWFLAKE_WAREHOUSE -q "PUT file://$(pwd)/$APP_FILE @$SNOWFLAKE_STAGE/streamlit_app.py OVERWRITE=TRUE AUTO_COMPRESS=FALSE"

# Create or replace the Streamlit app
echo -e "${YELLOW}Creating/replacing Streamlit app...${NC}"
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -d $SNOWFLAKE_DATABASE -s $SNOWFLAKE_SCHEMA -w $SNOWFLAKE_WAREHOUSE -q "CREATE OR REPLACE STREAMLIT SUPPORT_CONVERSATIONS_ANALYSIS FROM '@$SNOWFLAKE_STAGE' MAIN_FILE='streamlit_app.py'"

echo -e "${GREEN}Deployment complete!${NC}"
echo -e "${YELLOW}To access your app, go to Snowflake Snowsight > Streamlit Apps > SUPPORT_CONVERSATIONS_ANALYSIS${NC}"

# Make the script executable with: chmod +x deploy_to_snowflake.sh 