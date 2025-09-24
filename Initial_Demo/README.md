# Cortex Transcripts Demo
This is a Snowflake Cortex demo leveraging transcript data.  The data was originally generated using ChatGPT 4.0.  The notbook contains the promt that was used to create the data set.
## Set Up
### 1. Download the following files from this repo to your local computer
1. CORTEX_CALL_TRANSCRIPT_DEMO.ipynb
2. customer_support_calls.json
### 2. Run the SQL script below to set up the role "CORTEX_DEMO_ROLE" and warehouse "CORTEX_DEMO_WH" that will be able to interact with the demo
(NOTE:  You can copy this SQL and execute it in a Snowflake worksheet in Snowsight)
```SQL
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE CORTEX_DEMO_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'THIS IS USED FOR SNOWFLAKE DEMOS';

CREATE ROLE IF NOT EXISTS CORTEX_DEMO_ROLE;
CREATE DATABASE IF NOT EXISTS CORTEX;
CREATE SCHEMA IF NOT EXISTS CORTEX.TRANSCRIPTS;
CREATE STAGE IF NOT EXISTS CORTEX.TRANSCRIPTS.TRANSCRIPTS
    DIRECTORY = (ENABLE = TRUE);
GRANT USAGE ON DATABASE CORTEX TO ROLE CORTEX_DEMO_ROLE;
GRANT USAGE ON SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT USAGE ON WAREHOUSE CORTEX_DEMO_WH TO ROLE CORTEX_DEMO_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE CORTEX_DEMO_ROLE;
GRANT CREATE NOTEBOOK ON SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT OWNERSHIP ON DATABASE CORTEX TO ROLE CORTEX_DEMO_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE COPY CURRENT GRANTS;
GRANT CREATE DYNAMIC TABLE ON SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT OPERATE ON WAREHOUSE CORTEX_DEMO_WH TO ROLE CORTEX_DEMO_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CORTEX.TRANSCRIPTS TO ROLE CORTEX_DEMO_ROLE;

GRANT ROLE CORTEX_DEMO_ROLE TO USER <'Your User name'>;
```
### 3. Loading the Transcript Data into Snowflake
1. In Snowsight, navigate to the database and schema CORTEX.TRANSCRIPTS then to the stage TRANSCRIPTS
2. Navigate to the above stage, Click on the "+ Files" blue button in the top right
3. Drag and drop OR browse to the file 'customer_support_calls.json' and Click on the "Upload" button
4. Once the file has been uploaded, click on the ellipses > Load into Table
5. Keep the "+ Create new table" selected and under "Name" type "TRANSCRIPTS_RAW_JSON" then click "Next"
6. Since this file is already a JSON file, and we reference the column "variant_col" click "Load"
7.  Click "Done", Unless you want to open a worksheet and query the data in your new table. 

### 4. Loading the Notebook file into Snowflake
1. In Snowsight, on the Left Side menu pane, Select Projects > Notebooks
2. In the upper right corner of the Notebooks space, there is a blue button with "+ Notebooks | ⌄ "
3. Click on the "⌄" > "Import .ipynb file" and select "CORTEX_CALL_TRANSCRIPT_DEMO.ipynb" from where ever you downloaded it in step 1 above

### 5. Creating a Streamlit-in-Snowflake (SiS) Application
Below is the code that you will need to paste into a new SiS application within Snowflake.  In addition to pasting this code into the edit pane within SiS, you will also want to make sure you have the correct python packages available.
``` Python
# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
import pandas as pd

# Use your existing Snowflake account
session = get_active_session()

#UPDATE THE PAGE CONFIG SETTINGS
st.set_page_config(layout="wide")

def main():

    col1, col2 = st.columns([.25,.75])
    with col1:
        st.image("https://pbs.twimg.com/profile_images/1676633027647246337/_3-XUmcR_400x400.png",width=175)
    with col2:
        st.title("Snowflake Cortex - Transcripts")

    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        #Query for Barchart
        st.write("Transcripts by Category")
        bar_sql = f"""SELECT "PRODUCT_CATEGORY", COUNT("ID") AS CNT FROM TRANSCRIPTS_CORTEX_FINAL GROUP BY "PRODUCT_CATEGORY" ORDER BY 2 DESC"""
        bdf = session.sql(bar_sql)
        st.bar_chart(data=(bdf),x="PRODUCT_CATEGORY",height=300, width=100)
               
    with col2:
        st.write("Transcript Sentiment")
        bar_sql = f"""SELECT "SENTIMENT_TRANSCRIPT", COUNT("ID") AS CNT FROM TRANSCRIPTS_CORTEX_FINAL GROUP BY "SENTIMENT_TRANSCRIPT" ORDER BY 1 DESC"""
        bdf = session.sql(bar_sql)
        st.bar_chart(
            data=(bdf),
            #x="CNT",
            y="SENTIMENT_TRANSCRIPT",
            height=300,
            width=100,
            use_container_width=True,
            color="#3fb4d5"
        )

    with col3:
        #Query for Barchart
        st.write("Interaction Ratings")
        bar_sql = f"""SELECT "INTERACTION_RATING_VALUE", COUNT("ID") AS CNT FROM TRANSCRIPTS_CORTEX_FINAL GROUP BY "INTERACTION_RATING_VALUE" ORDER BY 2 DESC"""
        bdf = session.sql(bar_sql)
        st.bar_chart(data=(bdf),x="INTERACTION_RATING_VALUE",height=300, width=100)
                     #color="#3399FF" or #0099FF)
        
    #Selectbox to review a specific feature
    df = session.sql(f"""SELECT DISTINCT PRODUCT_CATEGORY FROM TRANSCRIPTS_CORTEX_FINAL ORDER BY 1 ASC""")
    trans_cat = st.selectbox("Transcription Category:", df.select("PRODUCT_CATEGORY").order_by("PRODUCT_CATEGORY", ascending=True))
    
    sql = session.sql("SELECT PRODUCT_CATEGORY as CATEGORY, TOPIC_SUMMARY as TOPIC, INTERACTION_RATING_VALUE as RATING, INTERACTION_REASON as REASON FROM TRANSCRIPTS_CORTEX_FINAL")
    st.dataframe(
        sql.filter(F.col("CATEGORY") == trans_cat),
        use_container_width=True
    )

#Query for the feature list    
    with st.expander("View ALL Transcripts"):
        feat_list = f"""SELECT * FROM TRANSCRIPTS_CORTEX_FINAL ORDER BY 1 ASC"""
        df = session.sql(feat_list).to_pandas()
        st.dataframe(df)
   

if __name__ == '__main__':
    main()
```
