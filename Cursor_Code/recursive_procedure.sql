-- Create a recursive stored procedure to call Process_New_Conversation() multiple times
CREATE OR REPLACE PROCEDURE Run_Multiple_Conversations(counter INTEGER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  result VARCHAR;
BEGIN
  -- Call the Process_New_Conversation procedure
  CALL Cursor_Demo.DATA_PREP.PROCESS_NEW_CONVERSATION() INTO :result;
  
  -- Log the execution for tracking
  INSERT INTO Cursor_Demo.Data_Prep.procedure_execution_log (execution_number, timestamp, result)
  VALUES (:counter, CURRENT_TIMESTAMP(), :result);
  
  -- Recursive call with decrement until we reach 0
  IF (counter > 1) THEN
    -- Recursive call with counter - 1
    RETURN Run_Multiple_Conversations(counter - 1);
  ELSE
    -- Base case: we've run the procedure the required number of times
    RETURN 'Completed ' || (6 - counter) || ' executions of Process_New_Conversation procedure';
  END IF;
END;
$$;

-- Create a logging table to track executions
CREATE OR REPLACE TABLE Cursor_Demo.Data_Prep.procedure_execution_log (
  execution_id INT AUTOINCREMENT PRIMARY KEY,
  execution_number INT,
  timestamp TIMESTAMP_NTZ,
  result VARCHAR(1000)
);

-- Simple wrapper to execute exactly 5 times
CREATE OR REPLACE PROCEDURE Run_Five_Conversations()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  result VARCHAR;
BEGIN
  -- Call the recursive procedure with initial counter = 5
  CALL Run_Multiple_Conversations(5) INTO :result;
  RETURN result;
END;
$$;

-- Update the PROCESS_NEW_CONVERSATION procedure to add recursion capabilities
CREATE OR REPLACE PROCEDURE Cursor_Demo.DATA_PREP.PROCESS_NEW_CONVERSATION(runs INTEGER DEFAULT 5)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    generate_transcript_result VARCHAR;
    export_json_result VARCHAR;
    result VARCHAR;
    conversation_id INT;
    random_id INT;
    current_run_result VARCHAR;
BEGIN
    -- Generate a random 4+ digit ID (between 1000 and 999999999)
    random_id := 1000 + MOD(ABS(RANDOM()), 999999000);
    
    -- Step 1: Insert a new record in SUPPORT_CONVERSATIONS_ADD_RECORDS with the custom ID
    INSERT INTO Cursor_Demo.DATA_PREP.SUPPORT_CONVERSATIONS_ADD_RECORDS (
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
        Cursor_Demo.V1.HOME_MEDICAL_DEVICES hmd ON hmd.DEVICE_ID = rd.RANDOM_DEVICE_ID;
    
    -- Store the conversation ID
    conversation_id := random_id;
    
    -- Step 2: Call the GENERATE_TRANSCRIPTS_ADD_RECORDS procedure
    generate_transcript_result := (CALL Cursor_Demo.DATA_PREP.GENERATE_TRANSCRIPTS_ADD_RECORDS());
    
    -- Step 3: Insert the new record into SUPPORT_CONV_INITIAL
    INSERT INTO Cursor_Demo.DATA_PREP.support_conv_initial (
        CONVERSATION_ID,
        START_TIME,
        END_TIME,
        AGENT_NAME,
        CUSTOMER_NAME,
        TRANSCRIPT
    )
    SELECT 
        sc.CONVERSATION_ID,
        sc.START_TIME,
        sc.END_TIME,
        sa.AGENT_NAME AS AGENT_NAME,
        c.CUSTOMER_NAME AS CUSTOMER_NAME,
        sc.TRANSCRIPT
    FROM 
        Cursor_Demo.DATA_PREP.SUPPORT_CONVERSATIONS_ADD_RECORDS sc
        LEFT JOIN Cursor_Demo.V1.SUPPORT_AGENTS sa ON sa.AGENT_ID = sc.AGENT_ID
        LEFT JOIN Cursor_Demo.V1.CUSTOMERS c ON c.CUSTOMER_ID = sc.CUSTOMER_ID
    WHERE 
        sc.CONVERSATION_ID = :conversation_id;
    
    -- Step 4: Call the EXPORT_CONVERSATIONS_TO_JSON procedure
    export_json_result := (CALL Cursor_Demo.DATA_PREP.EXPORT_CONVERSATIONS_TO_JSON());
    
    -- Step 5: Truncate both tables
    TRUNCATE TABLE Cursor_Demo.DATA_PREP.SUPPORT_CONVERSATIONS_ADD_RECORDS;
    TRUNCATE TABLE Cursor_Demo.DATA_PREP.support_conv_initial;
    
    -- Prepare result for current run
    current_run_result := 'Successfully processed new conversation: ' || conversation_id || 
              '. ' || generate_transcript_result || 
              '. ' || export_json_result || 
              '. Tables have been truncated.';
    
    -- Log the current run to the execution log table
    INSERT INTO Cursor_Demo.Data_Prep.procedure_execution_log (execution_number, timestamp, result)
    VALUES ((6 - runs), CURRENT_TIMESTAMP(), current_run_result);
    
    -- Recursive part: if more runs are needed, call the procedure again
    IF (runs > 1) THEN
        -- Call recursively with one fewer run
        RETURN Cursor_Demo.DATA_PREP.PROCESS_NEW_CONVERSATION(runs - 1);
    ELSE
        -- Base case: we've done all the runs
        RETURN 'Completed all ' || (6 - runs) || ' executions. Last run: ' || current_run_result;
    END IF;
END;
$$;

-- Example of how to call the procedures:
-- CALL Cursor_Demo.DATA_PREP.PROCESS_NEW_CONVERSATION(); -- Will run 5 times (default)
-- CALL Cursor_Demo.DATA_PREP.PROCESS_NEW_CONVERSATION(3); -- Will run 3 times

-- Or use the wrapper procedures:
-- CALL Run_Multiple_Conversations(10); -- Runs 10 times
-- CALL Run_Five_Conversations(); -- Runs 5 times

-- To view execution logs:
-- SELECT * FROM Cursor_Demo.Data_Prep.procedure_execution_log ORDER BY execution_id; 