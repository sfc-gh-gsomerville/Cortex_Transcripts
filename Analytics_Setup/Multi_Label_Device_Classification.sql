-- Multi-Label Device Classification Solutions
-- SQL script for handling multiple device categories from AI_CLASSIFY with output_mode: 'multi'

-- Solution 1: Extract all labels as a comma-separated string
-- This is the most common approach for displaying multiple categories
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

-- Solution 2: Extract all labels as an array (useful for further processing)
SELECT
  conversation_id,
  AI_CLASSIFY(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care'], 
    {'output_mode': 'multi'}
  ) as device_category,
  device_category::VARIANT:labels AS parsed_device_category_array,
  ARRAY_SIZE(device_category::VARIANT:labels) AS num_categories
FROM parsed_transcripts
LIMIT 10;

-- Solution 3: Create separate rows for each label (useful for analytics)
-- This creates one row per device category, allowing for easier aggregation
SELECT
  conversation_id,
  AI_CLASSIFY(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care'], 
    {'output_mode': 'multi'}
  ) as device_category,
  label.value::STRING AS parsed_device_category,
  label.index AS category_rank
FROM parsed_transcripts,
LATERAL FLATTEN(input => device_category::VARIANT:labels) AS label
LIMIT 20;

-- Solution 4: Extract primary and secondary categories with fallback
-- This provides structured access to the most important categories
SELECT
  conversation_id,
  AI_CLASSIFY(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care'], 
    {'output_mode': 'multi'}
  ) as device_category,
  device_category::VARIANT:labels[0]::STRING AS primary_device_category,
  device_category::VARIANT:labels[1]::STRING AS secondary_device_category,
  CASE 
    WHEN ARRAY_SIZE(device_category::VARIANT:labels) > 1 
    THEN ARRAY_TO_STRING(device_category::VARIANT:labels, ' | ')
    ELSE device_category::VARIANT:labels[0]::STRING
  END AS all_device_categories,
  ARRAY_SIZE(device_category::VARIANT:labels) AS total_categories
FROM parsed_transcripts
LIMIT 10;

-- Solution 5: Advanced multi-label handling with confidence scores (if available)
-- This assumes the AI_CLASSIFY function also returns confidence scores
SELECT
  conversation_id,
  AI_CLASSIFY(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care'], 
    {'output_mode': 'multi'}
  ) as device_category,
  ARRAY_TO_STRING(device_category::VARIANT:labels, ', ') AS all_categories,
  -- Filter to only high-confidence categories if scores are available
  ARRAY_TO_STRING(
    ARRAY_CONSTRUCT_COMPACT(
      CASE WHEN device_category::VARIANT:scores[0]::FLOAT > 0.5 
           THEN device_category::VARIANT:labels[0]::STRING END,
      CASE WHEN device_category::VARIANT:scores[1]::FLOAT > 0.5 
           THEN device_category::VARIANT:labels[1]::STRING END,
      CASE WHEN device_category::VARIANT:scores[2]::FLOAT > 0.5 
           THEN device_category::VARIANT:labels[2]::STRING END
    ), ', '
  ) AS high_confidence_categories
FROM parsed_transcripts
LIMIT 10;

-- Create a view for easy access to multi-label device categories
CREATE OR REPLACE VIEW parsed_transcripts_multi_category AS
SELECT
  conversation_id,
  transcript,
  AI_CLASSIFY(
    transcript, 
    ['Diabetes', 'Respiratory', 'Mobility', 'Urology', 'Pain Management', 'Monitoring', 'Orthopedic', 'Nutrition', 'Infusion', 'Wound Care'], 
    {'output_mode': 'multi'}
  ) as device_category_raw,
  device_category_raw::VARIANT:labels[0]::STRING AS primary_category,
  CASE 
    WHEN ARRAY_SIZE(device_category_raw::VARIANT:labels) > 1 
    THEN ARRAY_TO_STRING(device_category_raw::VARIANT:labels, ', ')
    ELSE device_category_raw::VARIANT:labels[0]::STRING
  END AS all_categories,
  ARRAY_SIZE(device_category_raw::VARIANT:labels) AS category_count
FROM parsed_transcripts;

-- Query the view
SELECT * FROM parsed_transcripts_multi_category
WHERE category_count > 1  -- Only show records with multiple categories
LIMIT 10;

-- Analytics query: Count conversations by device category (accounting for multi-labels)
SELECT 
  category.value::STRING AS device_category,
  COUNT(*) AS conversation_count
FROM parsed_transcripts_multi_category,
LATERAL FLATTEN(input => device_category_raw::VARIANT:labels) AS category
GROUP BY device_category
ORDER BY conversation_count DESC;


