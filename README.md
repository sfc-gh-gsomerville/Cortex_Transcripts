# Cortex Transcripts Demo
This is a Snowflake Cortex demo leveraging transcript data.


## Streamlit-in-Snowflake Application
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
