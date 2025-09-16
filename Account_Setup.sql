-- Set Context to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Set account for Cross-region inferenc: Cross-region inference with 'AWS_US' or 'ANY_REGION' can work on a Snowflake account in Azure. Snowflake's Cortex AI features, including LLM functions, support cross-cloud and cross-region inference. This is managed by the CORTEX_ENABLED_CROSS_REGION account parameter.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Create MED_DEVICE_TRANSCRIPTS database and CREATE_TRANSCRIPTS schema if they don't exist
CREATE DATABASE IF NOT EXISTS MED_DEVICE_TRANSCRIPTS;
USE SCHEMA MED_DEVICE_TRANSCRIPTS.PUBLIC;
--CREATE SCHEMA IF NOT EXISTS MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS;

-- Create warehouse
CREATE OR REPLACE WAREHOUSE CORTEX_DEMO_WH WITH WAREHOUSE_SIZE='SMALL';

-- Create an API integration with Github
CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION_MED_DEVICE_TRANSCRIPTS
   api_provider = git_https_api
   api_allowed_prefixes = ('https://github.com/sfc-gh-gsomerville/Cortex_Transcripts')
   enabled = true
   comment='Git integration with Cortex Transcripts Demo Github Repository.';

-- Create the integration with the Github demo repository
CREATE OR REPLACE GIT REPOSITORY GITHUB_REPO_MED_DEVICE_TRANSCRIPTS
   ORIGIN = 'https://github.com/sfc-gh-gsomerville/Cortex_Transcripts' 
   API_INTEGRATION = 'GITHUB_INTEGRATION_MED_DEVICE_TRANSCRIPTS' 
   COMMENT = 'Github Repository ';

-- Fetch most recent files from Github repository
ALTER GIT REPOSITORY GITHUB_REPO_MED_DEVICE_TRANSCRIPTS FETCH;
