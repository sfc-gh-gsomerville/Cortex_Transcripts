-- Cortex_Analysis.sql
-- SQL script for analyzing transcript data using Cortex LLM functions

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
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care']
    )['label'] as device_category
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