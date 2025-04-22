# Medical Device Transcripts Database Setup

## Summary
This SQL script creates a comprehensive demo database for a medical device support system in Snowflake. It establishes a database and schema, creates tables for support agents, customers, medical devices, and support conversations, populates these tables with realistic data, and generates AI-powered conversation transcripts.  This setup provides a foundation for showcasing data processing, analytics, and AI capabilities in a healthcare support context.

## Detailed Steps

### 1. Database and Schema Creation
```sql
CREATE DATABASE IF NOT EXISTS MED_DEVICE_TRANSCRIPTS;
CREATE SCHEMA IF NOT EXISTS MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS;
```
Creates a dedicated database called `MED_DEVICE_TRANSCRIPTS` and a schema called `CREATE_TRANSCRIPTS` within it if they don't already exist. This provides the foundation for organizing all the demo objects.

### 2. Support Agents Table Creation
```sql
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.SUPPORT_AGENTS (
    AGENT_ID INT AUTOINCREMENT PRIMARY KEY,
    AGENT_NAME VARCHAR(100)
);
```
Creates a table to store information about customer support agents with:
- Auto-incrementing agent ID as the primary key
- Agent name field

### 3. Support Agents Data Population
```sql
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
```
Populates the SUPPORT_AGENTS table with 8 sample agent names, providing a diverse set of support representatives for the demo.

### 4. Customers Table Creation
```sql
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS (
    CUSTOMER_ID INT AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(100)
);
```
Creates a table to store information about customers with:
- Auto-incrementing customer ID as the primary key
- Customer name field

### 5. Customers Data Population
```sql
INSERT INTO MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS (CUSTOMER_NAME)
VALUES 
    ('John Doe'), ('Jane Smith'), ('Michael Johnson'), ('Emily Davis'), ('Chris Lee'),
    -- [additional customer names omitted for brevity]
    ('Steven Patterson');
```
Populates the CUSTOMERS table with 100 unique customer names, providing a substantial dataset for the demo.

### 6. Medical Devices Table Creation
```sql
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
```
Creates a comprehensive table for medical devices with:
- Auto-incrementing device ID as the primary key
- Device name, category, and subcategory
- Prescription requirement flag
- Insurance coverage information
- Maintenance frequency and lifespan
- Common issues that might require support
- Training requirement flag

### 7. Medical Devices Data Population
```sql
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
    -- [additional device entries omitted for brevity]
    ('CPAP Cleaning Device', 'Respiratory', 'Maintenance', FALSE, 'None', 'Monthly', 36, 'Insufficient sanitizing, water reservoir leaks, cycle failures', FALSE)
;
```
Populates the HOME_MEDICAL_DEVICES table with 50 detailed entries covering various medical device categories including:
- Diabetes management devices
- Respiratory equipment
- Mobility aids
- Wound care supplies
- Urology devices
- Pain management tools
- Monitoring devices
- Nutrition and feeding equipment
- Infusion therapy devices
- Orthopedic braces

### 8. Support Conversations Table Creation
```sql
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
```
Creates a table for storing support conversation data with:
- Auto-incrementing conversation ID as the primary key
- Start and end timestamps
- Foreign keys to agent and customer tables
- Sentiment and resolution status
- Device name and common issue
- Space for the full conversation transcript

### 9. Support Conversations Data Population
```sql
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
        -- Random customer ID between 1 and 100
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
```
Populates the SUPPORT_CONVERSATIONS table with 5 sample conversations using:
- Random timestamps within the last 30 days
- Random agent and customer assignments
- Random sentiment (positive, negative, neutral)
- Random resolution status (70% resolved, 30% unresolved)
- Random device assignments with corresponding common issues

### 10. Transcript Generation Procedure
```sql
CREATE OR REPLACE PROCEDURE MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.GENERATE_TRANSCRIPTS()
```
Creates a stored procedure that:
- Identifies records without transcripts
- Builds detailed prompts for Claude AI based on conversation context
- Uses Snowflake Cortex to generate realistic support conversation transcripts
- Updates each record with the generated transcript
- Returns a success message with the count of transcripts generated

### 11. Transcript Generation Execution
```sql
CALL MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.GENERATE_TRANSCRIPTS();
```
Executes the procedure to generate transcripts for all conversation records.

### 12. Results Verification
```sql
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
```
Queries the SUPPORT_CONVERSATIONS table to verify the generated data, joining with the agent and customer tables to display names instead of IDs.

### 13. Initial Data Table Creation
```sql
CREATE OR REPLACE TABLE MED_DEVICE_TRANSCRIPTS.Data_Prep.support_conv_initial(
    CONVERSATION_ID INT,
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    AGENT_NAME VARCHAR(100),
    CUSTOMER_NAME VARCHAR(100),
    TRANSCRIPT VARCHAR(20000)
);
```
Creates a simplified table in the Data_Prep schema for storing the initial conversation data with:
- Conversation ID
- Start and end times
- Agent and customer names (instead of IDs)
- The full conversation transcript

### 14. Initial Data Population
```sql
INSERT INTO MED_DEVICE_TRANSCRIPTS.Data_Prep.support_conv_initial (
    CONVERSATION_ID,
    START_TIME,
    END_TIME,
    AGENT_NAME,
    CUSTOMER_NAME,
    TRANSCRIPT)
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
        LEFT JOIN MED_DEVICE_TRANSCRIPTS.CREATE_TRANSCRIPTS.CUSTOMERS c ON c.CUSTOMER_ID = sc.CUSTOMER_ID;
```
