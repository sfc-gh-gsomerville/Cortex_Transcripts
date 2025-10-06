-- Set Context to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create MED_DEVICE_TRANSCRIPTS database and CREATE_TRANSCRIPTS schema if they don't exist
CREATE DATABASE IF NOT EXISTS MED_DEVICE_TRANSCRIPTS;
CREATE SCHEMA IF NOT EXISTS MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS;

-- Create agents table
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS (
    AGENT_ID INT AUTOINCREMENT PRIMARY KEY,
    AGENT_NAME VARCHAR(100)
);

-- Insert agents
INSERT INTO MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS (AGENT_NAME)
VALUES 
    ('Alice Johnson'),
    ('Bob Smith'),
    ('Charlie Brown'),
    ('Diana Prince'),
    ('Ethan Hunt'),
    ('Fiona Williams'),
    ('George Taylor'),
    ('Hannah Martinez');

-- Create customers table
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS (
    CUSTOMER_ID INT AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(100)
);

-- Insert customers (we'll insert 100 unique customers)
INSERT INTO MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS (CUSTOMER_NAME)
VALUES 
    ('John Doe'), ('Jane Smith'), ('Michael Johnson'), ('Emily Davis'), ('Chris Lee'),
    ('Jessica Taylor'), ('David Brown'), ('Sarah Wilson'), ('Daniel Martinez'), ('Laura Garcia'),
    ('Robert Anderson'), ('Jennifer Thomas'), ('William White'), ('Patricia Harris'), ('James Clark'),
    ('Linda Lewis'), ('Richard Walker'), ('Elizabeth Hall'), ('Joseph Allen'), ('Margaret Young'),
    ('Thomas King'), ('Susan Wright'), ('Charles Scott'), ('Karen Green'), ('Christopher Adams'),
    ('Nancy Baker'), ('Matthew Nelson'), ('Betty Carter'), ('Donald Mitchell'), ('Lisa Perez'),
    ('Mark Roberts'), ('Sandra Turner'), ('Paul Phillips'), ('Ashley Campbell'), ('Steven Parker'),
    ('Kimberly Evans'), ('Edward Edwards'), ('Donna Collins'), ('Ronald Stewart'), ('Michelle Morris'),
    ('Timothy Rogers'), ('Carol Cook'), ('Larry Morgan'), ('Kathleen Reed'), ('Jeffrey Bell'),
    ('Dorothy Murphy'), ('Jason Bailey'), ('Deborah Rivera'), ('Scott Cooper'), ('Sharon Richardson'),
    ('Eric Cox'), ('Cynthia Howard'), ('Gregory Ward'), ('Ruth Hughes'), ('Joshua Foster'),
    ('Rebecca Simmons'), ('Dennis Bryant'), ('Judith Russell'), ('Frank Griffin'), ('Mary Diaz'),
    ('Raymond Hayes'), ('Virginia Sanders'), ('Kevin Price'), ('Julie Bennett'), ('Brian Wood'),
    ('Shirley Barnes'), ('Gary Brooks'), ('Anna Ross'), ('Nicholas Henderson'), ('Stephanie Coleman'),
    ('Andrew Jenkins'), ('Helen Perry'), ('Patrick Powell'), ('Amy Long'), ('Kenneth Patterson'),
    ('Katherine Hughes'), ('Jonathan Flores'), ('Debra Butler'), ('Jerry Simmons'), ('Carolyn Foster'),
    ('Dennis Gonzales'), ('Christine Bryant'), ('Arthur Alexander'), ('Marie Russell'), ('Gerald Griffin'),
    ('Frances Diaz'), ('Peter Hayes'), ('Catherine Sanders'), ('Harold Price'), ('Ann Bennett'),
    ('Wayne Wood'), ('Joyce Barnes'), ('Terry Brooks'), ('Diane Ross'), ('Lawrence Henderson'),
    ('Gloria Coleman'), ('Sean Jenkins'), ('Evelyn Perry'), ('Ralph Powell'), ('Cheryl Long'),
    ('Steven Patterson');

-- Create table for top 50 medical devices and supplies
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.HOME_MEDICAL_DEVICES (
    DEVICE_ID INT AUTOINCREMENT PRIMARY KEY,
    DEVICE_NAME VARCHAR(100),
    CATEGORY VARCHAR(50),
    SUBCATEGORY VARCHAR(50),
    REQUIRES_PRESCRIPTION BOOLEAN,
    TYPICAL_INSURANCE_COVERAGE VARCHAR(50),
    MAINTENANCE_FREQUENCY VARCHAR(50),
    AVERAGE_LIFESPAN_MONTHS INT,
    COMMON_ISSUES VARCHAR(500),
    TRAINING_REQUIRED BOOLEAN
);

-- Insert top 50 most common medical devices and supplies used by patients at home
INSERT INTO MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.HOME_MEDICAL_DEVICES (
    DEVICE_NAME,
    CATEGORY,
    SUBCATEGORY,
    REQUIRES_PRESCRIPTION,
    TYPICAL_INSURANCE_COVERAGE,
    MAINTENANCE_FREQUENCY,
    AVERAGE_LIFESPAN_MONTHS,
    COMMON_ISSUES,
    TRAINING_REQUIRED
)
VALUES
    -- Diabetes Management
    ('Blood Glucose Meter', 'Diabetes', 'Monitoring', FALSE, 'Partial', 'Weekly', 36, 'Battery issues, calibration errors, display malfunctions', FALSE),
    ('Insulin Pump', 'Diabetes', 'Treatment', TRUE, 'Full', 'Monthly', 48, 'Infusion site problems, occlusion alarms, battery failures', TRUE),
    ('Continuous Glucose Monitor (CGM)', 'Diabetes', 'Monitoring', TRUE, 'Partial', 'Weekly', 12, 'Sensor errors, adhesive issues, transmitter failures', TRUE),
    ('Insulin Pen', 'Diabetes', 'Treatment', TRUE, 'Partial', 'Monthly', 12, 'Dosage display issues, injection mechanism failures', FALSE),
    ('Lancet Device', 'Diabetes', 'Monitoring', FALSE, 'Partial', 'Monthly', 24, 'Spring mechanism failures, depth adjustment problems', FALSE),
    ('Diabetes Test Strips', 'Diabetes', 'Supplies', FALSE, 'Full', 'Daily', 1, 'Expiration, contamination, storage issues', FALSE),
    
    -- Respiratory
    ('Home Oxygen Concentrator', 'Respiratory', 'Oxygen Therapy', TRUE, 'Full', 'Monthly', 60, 'Filter clogging, compressor failure, decreased oxygen output', TRUE),
    ('Portable Oxygen Concentrator', 'Respiratory', 'Oxygen Therapy', TRUE, 'Partial', 'Monthly', 48, 'Battery issues, alarm malfunctions, decreased portability', TRUE),
    ('CPAP Machine', 'Respiratory', 'Sleep Apnea', TRUE, 'Full', 'Weekly', 60, 'Mask leaks, pressure inconsistencies, humidifier malfunctions', TRUE),
    ('Nebulizer', 'Respiratory', 'Medication Delivery', TRUE, 'Partial', 'Weekly', 36, 'Compressor failure, tubing leaks, medication cup cracks', FALSE),
    ('CPAP Masks', 'Respiratory', 'Sleep Apnea', TRUE, 'Partial', 'Monthly', 6, 'Fit issues, seal leaks, strap deterioration', FALSE),
    ('Incentive Spirometer', 'Respiratory', 'Lung Exercise', FALSE, 'Minimal', 'Monthly', 12, 'Flow indicator sticking, cracked chambers', FALSE),
    ('Pulse Oximeter', 'Respiratory', 'Monitoring', FALSE, 'Partial', 'Monthly', 24, 'Sensor inaccuracy, display failures, battery issues', FALSE),
    ('Oxygen Tubing', 'Respiratory', 'Supplies', TRUE, 'Full', 'Monthly', 3, 'Kinking, cracking, connector loosening', FALSE),
    
    -- Mobility Aids
    ('Standard Wheelchair', 'Mobility', 'Wheelchairs', FALSE, 'Full', 'Monthly', 60, 'Wheel alignment, brake failure, upholstery wear', FALSE),
    ('Power Wheelchair', 'Mobility', 'Wheelchairs', TRUE, 'Partial', 'Monthly', 72, 'Battery issues, controller malfunctions, motor failures', TRUE),
    ('Walker', 'Mobility', 'Ambulatory Aids', FALSE, 'Full', 'Every 3 months', 36, 'Joint loosening, handle grip wear, folding mechanism problems', FALSE),
    ('Cane', 'Mobility', 'Ambulatory Aids', FALSE, 'Partial', 'Every 6 months', 48, 'Tip wear, shaft bending, handle loosening', FALSE),
    ('Hospital Bed', 'Mobility', 'Furniture', TRUE, 'Full', 'Every 3 months', 84, 'Motor failure, control malfunction, frame issues', FALSE),
    ('Patient Lift', 'Mobility', 'Transfer Equipment', TRUE, 'Partial', 'Monthly', 60, 'Hydraulic failures, sling attachment issues, base instability', TRUE),
    ('Transfer Board', 'Mobility', 'Transfer Equipment', FALSE, 'Minimal', 'Every 6 months', 36, 'Surface smoothness degradation, cracking, splintering', FALSE),
    ('Knee Scooter', 'Mobility', 'Ambulatory Aids', FALSE, 'Partial', 'Every 3 months', 24, 'Wheel alignment, brake failures, steering column issues', FALSE),
    
    -- Wound Care
    ('Wound Dressing Supplies', 'Wound Care', 'Dressings', FALSE, 'Full', 'Daily', 1, 'Adhesive failure, premature saturation, skin irritation', FALSE),
    ('Negative Pressure Wound Therapy Device', 'Wound Care', 'Advanced Therapy', TRUE, 'Full', 'Weekly', 36, 'Vacuum seal leaks, canister full alerts, battery failures', TRUE),
    ('Compression Stockings', 'Wound Care', 'Compression Therapy', TRUE, 'Partial', 'Monthly', 6, 'Elasticity loss, seam tearing, sizing issues', FALSE),
    ('Compression Pump', 'Wound Care', 'Compression Therapy', TRUE, 'Partial', 'Monthly', 48, 'Pressure inconsistencies, sleeve leaks, controller errors', TRUE),
    ('Wound Cleansing Solutions', 'Wound Care', 'Supplies', FALSE, 'Minimal', 'Daily', 1, 'Contamination, expiration, container leakage', FALSE),
    
    -- Urology
    ('Urinary Catheter', 'Urology', 'Catheters', TRUE, 'Full', 'Daily', 1, 'Blockage, leakage, infection risk', TRUE),
    ('Catheter Insertion Supplies', 'Urology', 'Supplies', TRUE, 'Full', 'Daily', 1, 'Sterility concerns, packaging damage, expiration', FALSE),
    ('Bedside Drainage Bag', 'Urology', 'Collection', TRUE, 'Full', 'Weekly', 3, 'Leaking, tube kinking, valve malfunctions', FALSE),
    ('Leg Drainage Bag', 'Urology', 'Collection', TRUE, 'Full', 'Weekly', 3, 'Strap comfort issues, valve leakage, capacity limitations', FALSE),
    ('Incontinence Supplies', 'Urology', 'Incontinence', FALSE, 'Partial', 'Daily', 1, 'Leakage, skin irritation, odor control', FALSE),
    
    -- Pain Management
    ('TENS Unit', 'Pain Management', 'Electrotherapy', FALSE, 'Minimal', 'Monthly', 36, 'Electrode adhesion, lead wire breakage, intensity control issues', FALSE),
    ('Heat Therapy Pad', 'Pain Management', 'Thermal Therapy', FALSE, 'Minimal', 'Monthly', 24, 'Heating element failure, controller issues, auto-shutoff malfunction', FALSE),
    ('Cold Therapy System', 'Pain Management', 'Thermal Therapy', FALSE, 'Minimal', 'Monthly', 24, 'Leaking, pump failure, pad cracking', FALSE),
    ('Medication Dispenser', 'Pain Management', 'Medication', FALSE, 'Minimal', 'Weekly', 36, 'Alarm failures, compartment opening difficulties, battery issues', FALSE),
    
    -- Monitoring Devices
    ('Home Blood Pressure Monitor', 'Monitoring', 'Cardiovascular', FALSE, 'Minimal', 'Monthly', 36, 'Cuff leaks, pressure inaccuracy, display errors', FALSE),
    ('Digital Thermometer', 'Monitoring', 'Temperature', FALSE, 'None', 'Every 6 months', 24, 'Battery failure, calibration drift, broken tip', FALSE),
    ('Weight Scale', 'Monitoring', 'Weight', FALSE, 'Minimal', 'Yearly', 48, 'Calibration drift, display failure, platform cracking', FALSE),
    ('ECG Monitor', 'Monitoring', 'Cardiovascular', TRUE, 'Partial', 'Monthly', 36, 'Lead detachment, recording errors, transmission failures', TRUE),
    
    -- Nutrition & Feeding
    ('Feeding Tube Supplies', 'Nutrition', 'Enteral Feeding', TRUE, 'Full', 'Daily', 1, 'Tube clogging, connection leaks, site irritation', TRUE),
    ('Enteral Feeding Pump', 'Nutrition', 'Enteral Feeding', TRUE, 'Full', 'Monthly', 48, 'Alarm errors, flow rate inaccuracies, battery issues', TRUE),
    ('Nutrition Formula', 'Nutrition', 'Enteral Feeding', TRUE, 'Full', 'Daily', 1, 'Spoilage, digestive intolerance, mixing errors', FALSE),
    
    -- Infusion Therapy
    ('Infusion Pump', 'Infusion', 'IV Therapy', TRUE, 'Full', 'Weekly', 48, 'Occlusion alarms, air-in-line alerts, battery failures', TRUE),
    ('IV Supplies', 'Infusion', 'Supplies', TRUE, 'Full', 'Daily', 1, 'Contamination risks, expiration, packaging integrity', FALSE),
    ('Subcutaneous Infusion Set', 'Infusion', 'Supplies', TRUE, 'Partial', 'Every 3 days', 1, 'Site irritation, cannula kinking, adhesive failure', FALSE),
    
    -- Orthopedic
    ('Knee Brace', 'Orthopedic', 'Braces', FALSE, 'Partial', 'Monthly', 12, 'Strap wear, hinge failures, sizing issues', FALSE),
    ('Back Brace', 'Orthopedic', 'Braces', FALSE, 'Partial', 'Monthly', 12, 'Support deterioration, fastener failures, comfort issues', FALSE),
    ('Cervical Collar', 'Orthopedic', 'Braces', FALSE, 'Partial', 'Monthly', 6, 'Padding compression, fastener failure, fit issues', FALSE),
    ('CPAP Cleaning Device', 'Respiratory', 'Maintenance', FALSE, 'None', 'Monthly', 36, 'Insufficient sanitizing, water reservoir leaks, cycle failures', FALSE)
;

-- Create transcript table
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_CONVERSATIONS (
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
-- Alternative approach: First insert the basic data without transcripts, then update with transcripts
-- Step 1: Insert basic conversation details
INSERT INTO MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_CONVERSATIONS (
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
        -- Random timestamp within the last 30 days for start time
        DATEADD(minute, -1 * MOD(ABS(RANDOM()), 43200), CURRENT_TIMESTAMP()) AS START_TIME,
        -- End time between 2 and 20 minutes after start time
        DATEADD(minute, 2 + MOD(ABS(RANDOM()), 18), START_TIME) AS END_TIME,
        -- Random agent ID between 1 and 8
        1 + MOD(ABS(RANDOM()), 8) AS AGENT_ID,
        -- Sequential customer IDs from 1 to 100
        --ROW_NUMBER() OVER (ORDER BY SEQ8()) AS CUSTOMER_ID, -->  This was the original code the breaks beyond row ID > 111
        1 + MOD(ABS(RANDOM()), 100) AS CUSTOMER_ID, --Needed to update this so that is chose between 1 and 100 and wasn't dependent up on the row ID
        -- Random sentiment (positive, negative, neutral)
        CASE MOD(ABS(RANDOM()), 3)
            WHEN 0 THEN 'positive'
            WHEN 1 THEN 'negative'
            ELSE 'neutral'
        END AS SENTIMENT,
        -- Random resolution status (70% resolved, 30% unresolved)
        CASE WHEN RANDOM() < 0.7 THEN TRUE ELSE FALSE END AS ISSUE_RESOLVED,
        -- Random device ID between 1 and 50 - same value used for both fields
        1 + MOD(ABS(RANDOM()), 50) AS RANDOM_DEVICE_ID
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 5))
)
SELECT
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

-- Step 2: Now update each row with a transcript using a stored procedure
CREATE OR REPLACE PROCEDURE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.GENERATE_TRANSCRIPTS()
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
    FROM MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_CONVERSATIONS SC
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
        UPDATE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_CONVERSATIONS 
        SET TRANSCRIPT = :transcript
        WHERE CONVERSATION_ID = :curr_conversation_id;
        
        -- Increment counter
        row_count := row_count + 1;
    END FOR;
    
    RETURN 'Successfully generated ' || row_count || ' transcripts';
END;
$$;

-- Call the procedure to generate transcripts
CALL MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.GENERATE_TRANSCRIPTS();

-- Query to check results
SELECT 
    CONVERSATION_ID,
    START_TIME,
    END_TIME,
    sa.AGENT_NAME AS AGENT_NAME,
    c.CUSTOMER_NAME AS CUSTOMER_NAME,
    DEVICE_NAME,
    SUBSTRING(COMMON_ISSUE, 1, 50) AS ISSUE,
    SENTIMENT,
    ISSUE_RESOLVED,
    TRANSCRIPT
FROM 
    MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_CONVERSATIONS sc
    LEFT JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS sa ON sa.AGENT_ID = sc.AGENT_ID
    LEFT JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS c ON c.CUSTOMER_ID = sc.CUSTOMER_ID   
LIMIT 10;
