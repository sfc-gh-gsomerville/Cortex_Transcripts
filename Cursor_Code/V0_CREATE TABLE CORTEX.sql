--  This was the initial SQL script that did not work.  With Cortex Complte you are not able to return beyond ~8,000 tokens. 100 transcript records would exceed that number of tokens so the query with Cortex complete creates the first 2-3 records and then stops.

CREATE TABLE CORTEX.TRANSCRIPTS.AGENTS (
    ID INT AUTOINCREMENT PRIMARY KEY,
    Name STRING
);

INSERT INTO CORTEX.TRANSCRIPTS.AGENTS (Name)
VALUES 
    ('Alice Johnson'),
    ('Bob Smith'),
    ('Charlie Brown'),
    ('Diana Prince'),
    ('Ethan Hunt');

CREATE TABLE CORTEX.TRANSCRIPTS.CUSTOMERS (
    ID INT AUTOINCREMENT PRIMARY KEY,
    Name STRING
);

INSERT INTO CORTEX.TRANSCRIPTS.CUSTOMERS (Name)
VALUES 
    ('John Doe'), ('Jane Smith'), ('Michael Johnson'), ('Emily Davis'), ('Chris Lee'),
    ('Jessica Taylor'), ('David Brown'), ('Sarah Wilson'), ('Daniel Martinez'), ('Laura Garcia'),
    -- Add more names to reach 100
    ('Sophia Anderson'), ('James Thomas'), ('Olivia White'), ('Liam Harris'), ('Emma Clark'),
    ('Noah Lewis'), ('Ava Walker'), ('William Hall'), ('Isabella Allen'), ('Mason Young'),
    ('Mia King'), ('Lucas Wright'), ('Amelia Scott'), ('Ethan Green'), ('Harper Adams'),
    ('Logan Baker'), ('Evelyn Nelson'), ('Alexander Carter'), ('Abigail Mitchell'), ('Jacob Perez'),
    ('Charlotte Roberts'), ('Michael Turner'), ('Avery Phillips'), ('Benjamin Campbell'), ('Ella Parker'),
    ('Elijah Evans'), ('Scarlett Edwards'), ('Oliver Collins'), ('Grace Stewart'), ('Henry Morris'),
    ('Chloe Rogers'), ('Sebastian Cook'), ('Lily Morgan'), ('Jackson Reed'), ('Zoe Bell'),
    ('Aiden Murphy'), ('Hannah Bailey'), ('Matthew Rivera'), ('Lillian Cooper'), ('Samuel Richardson'),
    ('Aria Cox'), ('David Howard'), ('Sofia Ward'), ('Joseph Hughes'), ('Victoria Foster'),
    ('Carter Simmons'), ('Penelope Bryant'), ('Owen Russell'), ('Riley Griffin'), ('Wyatt Diaz'),
    ('Nora Hayes'), ('Jack Sanders'), ('Luna Price'), ('Luke Bennett'), ('Ellie Wood'),
    ('Gabriel Barnes'), ('Stella Brooks'), ('Caleb Ross'), ('Natalie Henderson'), ('Isaac Coleman'),
    ('Addison Jenkins'), ('Ryan Perry'), ('Aubrey Powell'), ('Nathan Long'), ('Brooklyn Patterson'),
    ('Hunter Hughes'), ('Savannah Flores'), ('Levi Butler'), ('Avery Simmons'), ('Christian Foster'),
    ('Zoe Gonzales'), ('Julian Bryant'), ('Hazel Alexander'), ('Aaron Russell'), ('Violet Griffin'),
    ('Eli Diaz'), ('Aurora Hayes'), ('Landon Sanders'), ('Lillian Price'), ('Jonathan Bennett'),
    ('Paisley Wood'), ('Isaiah Barnes'), ('Skylar Brooks'), ('Charles Ross'), ('Lucy Henderson');

CREATE OR REPLACE TABLE CORTEX.TRANSCRIPTS.TRANSCRIPTS_VARIANT (
    ID INT AUTOINCREMENT PRIMARY KEY,
    TRANSCRIPT VARCHAR
);


SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'CLAUDE-3-5-SONNET',
    'Generate 100 unique transcripts based on a conversation between an agent and a customer.  
     The customers are callign about medical devices and supplies that have been delivered directly to their homes.
     The conversation between the agent and the customer should be at least a 2 minute conversation.  
     The transcripts should be either positive, negative or neutral sentiment by the customer.  
     Not all issues chould be resolved.  The transcripts should also be unique conversations with complete realistic sentances. 
     The output should be in JSON format with an ID field and a transcript field.  No other text should be included in the output.')

-- First, ensure our stage exists
--CREATE OR REPLACE STAGE CORTEX.TRANSCRIPTS.TRANSCRIPTS;

-- First, ensure our stage exists
CREATE OR REPLACE STAGE CORTEX.TRANSCRIPTS.TRANSCRIPTS;

-- Generate transcripts and write directly to a CSV file in the stage
COPY INTO @CORTEX.TRANSCRIPTS.TRANSCRIPTS/transcripts.csv
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY NULL) AS id,
        SNOWFLAKE.CORTEX.COMPLETE(
            'CLAUDE-3-5-SONNET',
            'Generate 100 unique transcripts based on a conversation between an agent and a customer.  
            The customers are callign about medical devices and supplies that have been delivered directly to their homes.
            The conversation between the agent and the customer should be at least a 2 minute conversation.  
            The transcripts should have positive, negative or neutral sentiment by the customer.  
            Not all issues chould be resolved.  
            The transcripts should also be unique conversations with complete realistic sentances. 
            The output should be in JSON format with the fields ID, Customer, Agent, Start_Time, End_Time, and transcript.
            No other text should be included in the output.'
        ) AS transcript
    FROM TABLE(GENERATOR(ROWCOUNT => 1)) -- Generate 100 records
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE)
SINGLE = FALSE
OVERWRITE = TRUE
HEADER = TRUE;

--Trying to remove the Row_number over SQL
COPY INTO @CORTEX.TRANSCRIPTS.TRANSCRIPTS/transcripts.csv
FROM (
    SELECT
        SNOWFLAKE.CORTEX.COMPLETE(
            'CLAUDE-3-5-SONNET',
            'Generate 100 unique transcripts based on a conversation between an agent and a customer.  
            The customers are callign about medical devices and supplies that have been delivered directly to their homes.
            The conversation between the agent and the customer should be at least a 2 minute conversation.  
            The transcripts should have positive, negative or neutral sentiment by the customer.  
            Not all issues chould be resolved.  
            The transcripts should also be unique conversations with complete realistic sentances. 
            The output should be in JSON format with the fields ID, Customer, Agent, Start_Time, End_Time, and transcript.
            No other text should be included in the output.'
        ) AS transcript
    )
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE)
SINGLE = FALSE
OVERWRITE = TRUE
HEADER = TRUE;


SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'CLAUDE-3-5-SONNET',
    [
        {'role': 'system', 'content': 'Generate 100 unique transcripts based on a conversation between an agent and a customer.  
            The customers are callign about medical devices and supplies that have been delivered directly to their homes.
            The conversation between the agent and the customer should be at least a 2 minute conversation.  
            The transcripts should have positive, negative or neutral sentiment by the customer.  
            Not all issues chould be resolved.  
            The transcripts should also be unique conversations with complete realistic sentances. 
            The output should be in JSON format with the fields ID, Customer, Agent, Start_Time, End_Time, and transcript.
            No other text should be included in the output.' },
        {'role': 'user', 'content': 'this was really good'}
    ], {}
    ) as response;







SELECT
    t.value:ID::VARCHAR AS TRANSCRIPT_ID,
    t.value:Agent::VARCHAR AS AGENT_NAME,
    t.value:Customer::VARCHAR AS CUSTOMER_NAME,
    t.value:Start_Time::VARCHAR AS START_TIME,
    t.value:End_Time::VARCHAR AS END_TIME,
    t.value:Sentiment::VARCHAR AS SENTIMENT,
    t.value:Transcript::VARCHAR AS TRANSCRIPT_TEXT
FROM 
    CORTEX.TRANSCRIPTS.v1,
    LATERAL FLATTEN(PARSE_JSON(TRANSCRIPT):transcripts) t;

