-- Cortex_Analysis.sql
-- SQL script for analyzing transcript data using Cortex LLM functions

-- Set Context to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Setting Context
USE DATABASE MED_DEVICE_TRANSCRIPTS;
USE SCHEMA ANALYTICS;

-- Query using Cortex LLM function to summarize transcripts
SELECT
  conversation_id,
  transcript,
  SNOWFLAKE.CORTEX.SUMMARIZE(transcript) as transcript_summary
FROM parsed_transcripts
LIMIT 10;

-- Query using Cortex LLM function to analyze sentiment of transcripts
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.SENTIMENT(transcript) as sentiment_score,
  CASE
    WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript) > 0.33 THEN 'Positive'
    WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript) < -0.33 THEN 'Negative'
    ELSE 'Neutral'
  END as sentiment_category
FROM parsed_transcripts
LIMIT 10; 

-- Query using Cortex LLM function to classify transcripts into medical device categories
-- Original single-label classification
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care']
    )['label'] as device_category
FROM parsed_transcripts
LIMIT 10;

-- Multi-label classification using AI_CLASSIFY (if available)
-- This captures all applicable device categories, not just the first one
SELECT
  conversation_id,
  AI_CLASSIFY(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care'], 
    {'output_mode': 'multi'}
  ) as device_category,
  ARRAY_TO_STRING(device_category::VARIANT:labels, ', ') AS parsed_device_category
FROM parsed_transcripts
LIMIT 10;

-- Query using Cortex LLM function to extract the main issue from each transcript
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(transcript, 'What is the main issue?') as main_issue_json
FROM parsed_transcripts
LIMIT 10;

-- Query using Cortex LLM function with system prompt to determine if the issue was resolved
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    [
      {'role': 'system', 'content': 'You are a customer service quality analyst. 
        Analyze customer service transcripts and determine if the customer\'s issue was resolved. 
        Respond with exactly one word ("Resolved", "Unresolved", or "Partial") followed by a colon and 10 words or less explaining why.'},
      {'role': 'user', 'content': transcript}
    ],
    {'temperature': 0, 'max_tokens': 25}
  )['choices'][0]['messages']::STRING as resolution_with_reason
FROM parsed_transcripts
LIMIT 10;

-- Query using Cortex LLM function to rate customer service experience from 0-10
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT('Rate the customer service experience from 0 to 10, with 0 being very poor support without resolution 
    and 10 being highly supportive and complete resolution of the issue and a completely happy customer. 
    Return the results with a single integer for the rating followed by a colon and then a reason for the rating.
    The reason should be 25 words or less.', transcript)
  ) as customer_service_rating
FROM parsed_transcripts
LIMIT 10;

--creating a select stament that contains all of the Cortex LLM functions together
SELECT
  source,
  conversation_id,
  start_time,
  end_time,
  agent_name,
  customer_name,
  transcript,
  SNOWFLAKE.CORTEX.SUMMARIZE(transcript) as transcript_summary,
  SNOWFLAKE.CORTEX.SENTIMENT(transcript) as sentiment_score,
  CASE
    WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript) > 0.33 THEN 'Positive'
    WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript) < -0.33 THEN 'Negative'
    ELSE 'Neutral'
  END as sentiment_category,
  SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care']
    )['label'] as device_category,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(transcript, 'What is the main issue?') as main_issue_json,
    SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    [
      {'role': 'system', 'content': 'You are a customer service quality analyst. 
        Analyze customer service transcripts and determine if the customer\'s issue was resolved. 
        Respond with exactly one word ("Resolved", "Unresolved", or "Partial") followed by a colon and 10 words or less explaining why.'},
      {'role': 'user', 'content': transcript}
    ],
    {'temperature': 0, 'max_tokens': 25}
    )['choices'][0]['messages']::STRING as resolution_with_reason,
    SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT('Rate the customer service experience from 0 to 10, with 0 being very poor support without resolution 
    and 10 being highly supportive and complete resolution of the issue and a completely happy customer. 
    Return the results with a single integer for the rating followed by a colon and then a reason for the rating.
    The reason should be 25 words or less.', transcript)
  ) as customer_service_rating
FROM parsed_transcripts
LIMIT 10;

--Create a Dynamic Table of all of the Cortex LLM function fields combined with the original fields
CREATE OR REPLACE DYNAMIC TABLE transcript_analysis_results
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CORTEX_DEMO_WH
  REFRESH_MODE = 'AUTO'
AS
    SELECT
    source,
    conversation_id,
    start_time,
    end_time,
    agent_name,
    customer_name,
    transcript,
    SNOWFLAKE.CORTEX.SUMMARIZE(transcript) as transcript_summary,
    SNOWFLAKE.CORTEX.SENTIMENT(transcript) as sentiment_score,
    CASE
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript) > 0.33 THEN 'Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(transcript) < -0.33 THEN 'Negative'
        ELSE 'Neutral'
    END as sentiment_category,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        transcript, 
        ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care','Other']
        )['label'] as device_category,
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(transcript, 'What is the main issue?') as main_issue_json,
        SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        [
        {'role': 'system', 'content': 'You are a customer service quality analyst. 
            Analyze customer service transcripts and determine if the customer\'s issue was resolved. 
            Respond with exactly one word ("Resolved", "Unresolved", or "Partial") followed by a colon and 10 words or less explaining why.'},
        {'role': 'user', 'content': transcript}
        ],
        {'temperature': 0, 'max_tokens': 25}
        )['choices'][0]['messages']::STRING as resolution_with_reason,
        SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT('Rate the customer service experience from 0 to 10, with 0 being very poor support without resolution 
        and 10 being highly supportive and complete resolution of the issue and a completely happy customer. 
        Return the results with a single integer for the rating followed by a colon and then a reason for the rating.
        The reason should be 25 words or less.', transcript)
        ) as customer_service_rating
    FROM parsed_transcripts;

    --Top 10 rows of the Dynamic Table
    SELECT * FROM transcript_analysis_results
    LIMIT 10;

-- Query to work with MAIN_ISSUE_JSON and create new columns from the JSON
SELECT
  conversation_id,
  main_issue_json,
  main_issue_json[0]:answer::STRING as main_issue_answer,
  main_issue_json[0]:score::FLOAT as main_issue_score,
  CASE
    WHEN main_issue_json[0]:score::FLOAT >= 0.7 THEN 'High Confidence'
    WHEN main_issue_json[0]:score::FLOAT >= 0.3 THEN 'Medium Confidence'
    ELSE 'Low Confidence'
  END as main_issue_confidence_level
FROM transcript_analysis_results
LIMIT 10;

-- Create a Dynamic Table for main issue analysis
CREATE OR REPLACE DYNAMIC TABLE main_issue_analysis
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CORTEX_DEMO_WH
  REFRESH_MODE = 'AUTO'
AS
  SELECT
    conversation_id,
    main_issue_json,
    main_issue_json[0]:answer::STRING as main_issue_answer,
    main_issue_json[0]:score::FLOAT as main_issue_score,
    CASE
      WHEN main_issue_json[0]:score::FLOAT >= 0.7 THEN 'High Confidence'
      WHEN main_issue_json[0]:score::FLOAT >= 0.3 THEN 'Medium Confidence'
      ELSE 'Low Confidence'
    END as main_issue_confidence_level
  FROM transcript_analysis_results;

-- Query the dynamic table
SELECT * FROM main_issue_analysis LIMIT 10;

-- Create a Dynamic Table for resolution and customer service analysis
CREATE OR REPLACE DYNAMIC TABLE resolution_service_analysis
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = CORTEX_DEMO_WH
  REFRESH_MODE = 'AUTO'
AS
  SELECT
    conversation_id,
    resolution_with_reason,
    SPLIT_PART(resolution_with_reason, ':', 1) as resolution,
    TRIM(SPLIT_PART(resolution_with_reason, ':', 2)) as resolution_reason,
    customer_service_rating,
    SPLIT_PART(customer_service_rating, ':', 1) as service_rating,
    TRIM(SPLIT_PART(customer_service_rating, ':', 2)) as service_rating_reason
  FROM transcript_analysis_results;

-- Query the resolution and customer service dynamic table
SELECT * FROM resolution_service_analysis LIMIT 10;

-- Combined query that joins all three dynamic tables
SELECT
  t.source,
  t.conversation_id,
  t.start_time,
  t.end_time,
  t.agent_name,
  t.customer_name,
  t.transcript,
  t.transcript_summary,
  t.sentiment_score,
  t.sentiment_category,
  t.device_category,
  --t.main_issue_json,
  m.main_issue_answer,
  m.main_issue_score,
  m.main_issue_confidence_level,
  --r.resolution_with_reason,
  r.resolution,
  r.resolution_reason,
  --r.customer_service_rating,
  r.service_rating,
  r.service_rating_reason
FROM transcript_analysis_results t
JOIN main_issue_analysis m ON t.conversation_id = m.conversation_id
JOIN resolution_service_analysis r ON t.conversation_id = r.conversation_id
LIMIT 10;

-- Create a Dynamic Table that combines all three analysis tables
CREATE OR REPLACE DYNAMIC TABLE TRANSCRIPT_ANALYSIS_RESULTS_FINAL
  TARGET_LAG = '1 MINUTE'
  WAREHOUSE = CORTEX_DEMO_WH
  REFRESH_MODE = 'AUTO'
AS
  SELECT
    t.source,
    t.conversation_id,
    t.start_time,
    t.end_time,
    t.agent_name,
    t.customer_name,
    t.transcript,
    t.transcript_summary,
    t.sentiment_score,
    t.sentiment_category,
    t.device_category::VARCHAR as device_category,
    m.main_issue_answer,
    m.main_issue_score,
    m.main_issue_confidence_level,
    r.resolution,
    r.resolution_reason,
    r.service_rating,
    r.service_rating_reason
  FROM transcript_analysis_results t
  JOIN main_issue_analysis m ON t.conversation_id = m.conversation_id
  JOIN resolution_service_analysis r ON t.conversation_id = r.conversation_id;

-- Query the combined dynamic table
SELECT * FROM TRANSCRIPT_ANALYSIS_RESULTS_FINAL LIMIT 10;

/* Create or replace the existing table with all transcripts and then copy over all records from the Dynamic Table.  
This has to be done bacause Cortex Search can not be used ontop of a Dynamic Table
***NOTE*** This query must be run manually to refreshed each time new records are generated (I haven't built an update pipline yet!)*/
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.ANALYTICS.TRANSCRIPT_ANALYSIS_RESULTS_FINAL_TBL AS 
SELECT * FROM MED_DEVICE_TRANSCRIPTS.ANALYTICS.TRANSCRIPT_ANALYSIS_RESULTS_FINAL;
