# Cortex Analysis Documentation

## Summary

This document provides an overview of the `Cortex_Analysis.sql` script, which leverages Snowflake's Cortex LLM functions to analyze customer support call transcripts. The script performs various analyses including sentiment analysis, device categorization, issue extraction, resolution determination, and customer service rating. The results are organized into dynamic tables that automatically refresh when source data changes.

## Script Components

### 1. Individual Cortex LLM Function Queries

The script begins with several standalone queries that demonstrate individual Cortex LLM functions:

#### Transcript Summarization
```sql
SELECT
  conversation_id,
  transcript,
  SNOWFLAKE.CORTEX.SUMMARIZE(transcript) as transcript_summary
FROM parsed_transcripts
LIMIT 10;
```
This query uses the `SUMMARIZE` function to generate concise summaries of each transcript.

#### Sentiment Analysis
```sql
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
```
This query uses the `SENTIMENT` function to analyze the emotional tone of each transcript and categorize it as Positive, Negative, or Neutral based on score thresholds.

#### Device Categorization
```sql
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care']
    )['label'] as device_category
FROM parsed_transcripts
LIMIT 10;
```
This query uses the `CLASSIFY_TEXT` function to categorize each transcript into one of several medical device categories.

#### Main Issue Extraction
```sql
SELECT
  conversation_id,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(transcript, 'What is the main issue?') as main_issue_json
FROM parsed_transcripts
LIMIT 10;
```
This query uses the `EXTRACT_ANSWER` function to identify the main issue discussed in each transcript, returning the result as a JSON object containing the answer and a confidence score.

#### Resolution Determination
```sql
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
```
This query uses the `COMPLETE` function with a system prompt to determine if the customer's issue was resolved, categorizing it as Resolved, Unresolved, or Partial, with a brief explanation.

#### Customer Service Rating
```sql
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
```
This query uses the `COMPLETE` function to rate the customer service experience on a scale of 0-10, with a brief explanation for the rating.

### 2. Combined Analysis Query

The script includes a query that combines all the Cortex LLM functions into a single result set:

```sql
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
      {'role': 'system', 'content': 'You are a customer service quality analyst...'},
      {'role': 'user', 'content': transcript}
    ],
    {'temperature': 0, 'max_tokens': 25}
  )['choices'][0]['messages']::STRING as resolution_with_reason,
  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT('Rate the customer service experience...', transcript)
  ) as customer_service_rating
FROM parsed_transcripts
LIMIT 10;
```

### 3. Dynamic Tables

The script creates several dynamic tables that automatically refresh when source data changes:

#### Transcript Analysis Results
```sql
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
        {'role': 'system', 'content': 'You are a customer service quality analyst...'},
        {'role': 'user', 'content': transcript}
      ],
      {'temperature': 0, 'max_tokens': 25}
    )['choices'][0]['messages']::STRING as resolution_with_reason,
    SNOWFLAKE.CORTEX.COMPLETE(
      'mistral-large2',
      CONCAT('Rate the customer service experience...', transcript)
    ) as customer_service_rating
  FROM parsed_transcripts;
```

This dynamic table combines all the Cortex LLM functions into a single table that automatically refreshes when the source data changes.

#### Main Issue Analysis
```sql
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
```

This dynamic table extracts the main issue answer and score from the JSON field and adds a confidence level based on the score.

#### Resolution and Service Analysis
```sql
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
```

This dynamic table splits the resolution and customer service rating fields into separate columns for easier analysis.

#### Final Combined Analysis
```sql
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
```

This final dynamic table combines all three analysis tables into a single comprehensive view, with the device_category field explicitly cast to VARCHAR for better usability.

## Usage

The dynamic tables created by this script can be used for various analytical purposes:

1. **Transcript Analysis Results**: Provides a comprehensive view of all analyses performed on each transcript.
2. **Main Issue Analysis**: Focuses on the main issues identified in each conversation, with confidence levels.
3. **Resolution and Service Analysis**: Provides insights into resolution status and customer service ratings.
4. **Final Combined Analysis**: Offers a complete view of all analyses in a single table, optimized for reporting and dashboard creation.

Each dynamic table automatically refreshes when the source data changes, ensuring that analyses are always up-to-date. 