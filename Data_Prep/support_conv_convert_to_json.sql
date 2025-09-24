-- Reference SQL from create_cursor_demo_table.sql that was used to create and populate the SUPPORT_CONVERSATIONS table

-- Create DATA_PREP schema in MED_DEVICE_TRANSCRIPTS if it doesn't exist
CREATE SCHEMA IF NOT EXISTS MED_DEVICE_TRANSCRIPTS.DATA_PREP;

--Adding the data generated in the initial sql script for the data that will be used in the Cortex & SiS Demonstration
-- Create support_conv_initial table in the DATA_PREP schema from the data generated in the table SUPPORT_CONVERSATIONS as a CTAS
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.DATA_PREP.support_conv_initial AS
SELECT 
    CONVERSATION_ID,
    START_TIME,
    END_TIME,
    sa.AGENT_NAME AS AGENT_NAME,
    c.CUSTOMER_NAME AS CUSTOMER_NAME,
    TRANSCRIPT
FROM 
    MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_CONVERSATIONS sc
    LEFT JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS sa ON sa.AGENT_ID = sc.AGENT_ID
    LEFT JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS c ON c.CUSTOMER_ID = sc.CUSTOMER_ID  
;

-- Create a stage in the DATA_PREP schema for storing the JSON file created form the initial transcript data
CREATE OR REPLACE STAGE MED_DEVICE_TRANSCRIPTS.DATA_PREP.call_data_initial;

--Creating the JSON File of them initial Transcript Data created
COPY INTO @MED_DEVICE_TRANSCRIPTS.data_prep.call_data_initial/support_conv_initial.json 
FROM (
    SELECT OBJECT_CONSTRUCT('conversation_id', CONVERSATION_ID, 'start_time', START_TIME, 'end_time', END_TIME, 'agent_name', AGENT_NAME, 'customer_name', CUSTOMER_NAME, 'transcript', TRANSCRIPT) 
    FROM MED_DEVICE_TRANSCRIPTS.DATA_PREP.support_conv_initial) FILE_FORMAT = (TYPE = JSON) 
    OVERWRITE = TRUE
;

--Now we will create a data pipeline to generate new transcripts and export them to separate JSON files to create an ingestion pipeline
-- Create file format for JSON data
CREATE OR REPLACE FILE FORMAT MED_DEVICE_TRANSCRIPTS.DATA_PREP.json_format
  TYPE = JSON;

-- Create a stage in the DATA_PREP schema for storing call data
CREATE OR REPLACE STAGE MED_DEVICE_TRANSCRIPTS.DATA_PREP.call_data_new;

-- Create transcript table in the DATA_PREP schema
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.DATA_PREP.SUPPORT_CONVERSATIONS_NEW (
    CONVERSATION_ID INT AUTOINCREMENT PRIMARY KEY,
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    AGENT_ID INT,
    CUSTOMER_ID INT,
    SENTIMENT VARCHAR(20),
    ISSUE_RESOLVED BOOLEAN,
    DEVICE_NAME VARCHAR(100),
    COMMON_ISSUE VARCHAR(500),
    TRANSCRIPT VARCHAR(20000),
    FOREIGN KEY (AGENT_ID) REFERENCES MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS(AGENT_ID),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS(CUSTOMER_ID)
);

-- Create a new procedure to generate transcripts for the SUPPORT_CONVERSATIONS_NEW table
CREATE OR REPLACE PROCEDURE MED_DEVICE_TRANSCRIPTS.DATA_PREP.GENERATE_TRANSCRIPTS_NEW_RECORDS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    conv_cursor CURSOR FOR SELECT 
        CONVERSATION_ID, 
        SENTIMENT, 
        ISSUE_RESOLVED,
        C.CUSTOMER_NAME,
        A.AGENT_NAME,
        SC.DEVICE_NAME,
        SC.COMMON_ISSUE
    FROM MED_DEVICE_TRANSCRIPTS.DATA_PREP.SUPPORT_CONVERSATIONS_NEW SC
    JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS C ON SC.CUSTOMER_ID = C.CUSTOMER_ID
    JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS A ON SC.AGENT_ID = A.AGENT_ID
    WHERE TRANSCRIPT IS NULL;
    
    curr_conversation_id INTEGER;
    curr_sentiment VARCHAR;
    curr_resolved BOOLEAN;
    curr_customer_name VARCHAR;
    curr_agent_name VARCHAR;
    curr_device_name VARCHAR;
    curr_common_issue VARCHAR;
    
    prompt VARCHAR;
    transcript VARCHAR;
    row_count INTEGER DEFAULT 0;
BEGIN
    FOR record IN conv_cursor DO
        curr_conversation_id := record.CONVERSATION_ID;
        curr_sentiment := record.SENTIMENT;
        curr_resolved := record.ISSUE_RESOLVED;
        curr_customer_name := record.CUSTOMER_NAME;
        curr_agent_name := record.AGENT_NAME;
        curr_device_name := record.DEVICE_NAME;
        curr_common_issue := record.COMMON_ISSUE;
        
        -- Build the prompt for Claude
        prompt := CONCAT(
            'Generate a realistic transcript of a conversation between a customer support agent and a customer for a medical device company. ',
            'The conversation should be at least 2 minutes long, showing timestamps. ',
            'The customer is calling about their ', curr_device_name, ' with one of the following issues: ', curr_common_issue, '. ',
            'The sentiment should be ', curr_sentiment, '. ',
            'The issue should be ', CASE WHEN curr_resolved THEN 'resolved' ELSE 'unresolved' END, '. ',
            'Format it as a back-and-forth dialogue with timestamps, customer name, and agent name. ',
            'The customer name is ', curr_customer_name, ' ',
            'and the agent name is ', curr_agent_name, '. ',
            'Make the conversation detailed and realistic with complete sentences. ',
            'Only output the transcript itself, no additional text.'
        );
        
        -- Generate transcript using Cortex
        transcript := SNOWFLAKE.CORTEX.COMPLETE('CLAUDE-3-5-SONNET', prompt);
        
        -- Update the record with the generated transcript
        UPDATE MED_DEVICE_TRANSCRIPTS.DATA_PREP.SUPPORT_CONVERSATIONS_NEW 
        SET TRANSCRIPT = :transcript
        WHERE CONVERSATION_ID = :curr_conversation_id;
        
        -- Increment counter
        row_count := row_count + 1;
    END FOR;
    
    RETURN 'Successfully generated ' || row_count || ' transcripts';
END;
$$;

-- Call the procedure to generate transcripts
CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.GENERATE_TRANSCRIPTS_NEW_RECORDS();

-- Create a procedure to export conversation data to a JSON file with a timestamp in the filename
CREATE OR REPLACE PROCEDURE MED_DEVICE_TRANSCRIPTS.DATA_PREP.EXPORT_CONVERSATIONS_TO_JSON()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    timestamp_str VARCHAR;
    filename VARCHAR;
    query_text VARCHAR;
    result VARCHAR;
BEGIN
    -- Generate a timestamp string for the filename (format: YYYYMMDD_HHMMSS)
    timestamp_str := TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HHMMSS');
    filename := 'conversations_' || timestamp_str || '.json';
    
    -- Create a temporary table with the data in JSON format
    CREATE OR REPLACE TEMPORARY TABLE temp_json_data AS
    SELECT OBJECT_CONSTRUCT(
        'conversation_id', SC.CONVERSATION_ID,
        'start_time', SC.START_TIME,
        'end_time', SC.END_TIME,
        'agent_name', SA.AGENT_NAME,
        'customer_name', C.CUSTOMER_NAME,
        'transcript', SC.TRANSCRIPT
    ) AS json_data
    FROM MED_DEVICE_TRANSCRIPTS.DATA_PREP.support_conversations_new SC
    LEFT JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS SA ON SA.AGENT_ID = SC.AGENT_ID
    LEFT JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS C ON C.CUSTOMER_ID = SC.CUSTOMER_ID;
    
    -- Convert the rows to a JSON array
    query_text := 'COPY INTO @MED_DEVICE_TRANSCRIPTS.DATA_PREP.call_data_new/' || filename || ' 
    FROM (SELECT ARRAY_AGG(json_data) FROM temp_json_data)
    FILE_FORMAT = (FORMAT_NAME = ''MED_DEVICE_TRANSCRIPTS.DATA_PREP.json_format'')
    OVERWRITE = TRUE;';
    
    -- Execute the copy command
    EXECUTE IMMEDIATE :query_text;
    
    -- Return success message with the filename
    result := 'Successfully exported conversation data to ' || filename;
    RETURN result;
END;
$$;

-- Call the procedure to export conversation data to a JSON file
CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.EXPORT_CONVERSATIONS_TO_JSON();

-- Create a new procedure that performs the complete conversation processing pipeline
CREATE OR REPLACE PROCEDURE MED_DEVICE_TRANSCRIPTS.DATA_PREP.PROCESS_CONVERSATIONS_BATCH(NUM_EXECUTIONS INT DEFAULT 3)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    generate_transcript_result VARCHAR;
    export_json_result VARCHAR;
    results_array ARRAY DEFAULT ARRAY_CONSTRUCT();
    conversation_id INT;
    random_id INT;
    current_execution INT DEFAULT 1;
    final_result VARCHAR;
BEGIN
    -- Recursive execution loop
    REPEAT
        -- Generate a random 4+ digit ID (between 1000 and 999999999)
        random_id := 1000 + MOD(ABS(RANDOM()), 999999000);
        
        -- Step 1: Insert a new record in SUPPORT_CONVERSATIONS_NEW with the custom ID
        INSERT INTO MED_DEVICE_TRANSCRIPTS.DATA_PREP.SUPPORT_CONVERSATIONS_NEW (
            CONVERSATION_ID,
            START_TIME,
            END_TIME,
            AGENT_ID,
            CUSTOMER_ID,
            SENTIMENT,
            ISSUE_RESOLVED,
            DEVICE_NAME,
            COMMON_ISSUE
        )
        WITH random_data AS (
            SELECT
                -- Use our generated random ID
                :random_id AS CONVERSATION_ID,
                -- Random timestamp within the last 30 days for start time
                DATEADD(minute, -1 * MOD(ABS(RANDOM()), 43200), CURRENT_TIMESTAMP()) AS START_TIME,
                -- End time between 2 and 20 minutes after start time
                DATEADD(minute, 2 + MOD(ABS(RANDOM()), 18), START_TIME) AS END_TIME,
                -- Random agent ID between 1 and 8
                1 + MOD(ABS(RANDOM()), 8) AS AGENT_ID,
                -- Sequential customer IDs from 1 to 100
                1 + MOD(ABS(RANDOM()), 100) AS CUSTOMER_ID,
                -- Random sentiment (positive, negative, neutral)
                CASE MOD(ABS(RANDOM()), 3)
                    WHEN 0 THEN 'positive'
                    WHEN 1 THEN 'negative'
                    ELSE 'neutral'
                END AS SENTIMENT,
                -- Random resolution status (70% resolved, 30% unresolved)
                CASE WHEN RANDOM() < 0.7 THEN TRUE ELSE FALSE END AS ISSUE_RESOLVED,
                -- Random device ID between 1 and 50
                1 + MOD(ABS(RANDOM()), 50) AS RANDOM_DEVICE_ID
            FROM 
                TABLE(GENERATOR(ROWCOUNT => 1))
        )
        SELECT
            rd.CONVERSATION_ID,
            rd.START_TIME,
            rd.END_TIME,
            rd.AGENT_ID,
            rd.CUSTOMER_ID,
            rd.SENTIMENT,
            rd.ISSUE_RESOLVED,
            hmd.DEVICE_NAME,
            hmd.COMMON_ISSUES AS COMMON_ISSUE
        FROM 
            random_data rd
        JOIN 
            MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.HOME_MEDICAL_DEVICES hmd ON hmd.DEVICE_ID = rd.RANDOM_DEVICE_ID;
        
        -- Store the conversation ID
        conversation_id := random_id;
        
        -- Step 2: Call the GENERATE_TRANSCRIPTS_ADD_RECORDS procedure
        generate_transcript_result := (CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.GENERATE_TRANSCRIPTS_NEW_RECORDS());
               
        -- Step 3: Call the EXPORT_CONVERSATIONS_TO_JSON procedure
        export_json_result := (CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.EXPORT_CONVERSATIONS_TO_JSON());
        
        -- Step 4: Truncate both tables
        TRUNCATE TABLE MED_DEVICE_TRANSCRIPTS.DATA_PREP.SUPPORT_CONVERSATIONS_NEW;
        
        -- Add result to the array
        results_array := ARRAY_APPEND(:results_array, 'Execution ' || current_execution || ': Processed conversation ' || conversation_id);
        
        -- Increment execution counter
        current_execution := current_execution + 1;
        
    UNTIL (current_execution > NUM_EXECUTIONS)
    END REPEAT;
    
    -- Build final result string from array
    final_result := 'Successfully processed ' || NUM_EXECUTIONS || ' conversations.' || 
                    CHR(10) || ARRAY_TO_STRING(results_array, CHR(10));
    
    RETURN final_result;
END;
$$;

-- Call the new procedure to process conversations (default 3 times)
CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.PROCESS_CONVERSATIONS_BATCH();

-- Example with explicit execution count
-- CALL MED_DEVICE_TRANSCRIPTS.DATA_PREP.PROCESS_CONVERSATIONS_BATCH(5);

