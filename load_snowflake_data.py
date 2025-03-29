import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    # Get the account identifier from environment
    account = os.getenv('SNOWFLAKE_ACCOUNT', '')
    
    # Split the account identifier if it contains hyphens
    if '-' in account:
        parts = account.split('-')
        if len(parts) == 2:
            account = f"{parts[0]}-{parts[1]}"  # Use the format: ORGNAME-ACCOUNTNAME
    
    print(f"Connecting to Snowflake account: {account}")  # Debug print
    
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=account,
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    return conn

def load_support_conversations():
    """Load support conversations data into a pandas DataFrame"""
    try:
        # Create connection
        conn = get_snowflake_connection()
        
        # Query to get support conversations data
        query = """
        SELECT 
            sc.*,
            sa.AGENT_NAME,
            c.CUSTOMER_NAME
        FROM 
            Cursor_Demo.V1.SUPPORT_CONVERSATIONS sc
            JOIN Cursor_Demo.V1.SUPPORT_AGENTS sa ON sc.AGENT_ID = sa.AGENT_ID
            JOIN Cursor_Demo.V1.CUSTOMERS c ON sc.CUSTOMER_ID = c.CUSTOMER_ID
        """
        
        # Load data into DataFrame
        df = pd.read_sql(query, conn)
        
        # Close connection
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return None

if __name__ == "__main__":
    # Load the data
    df = load_support_conversations()
    
    if df is not None:
        print("\nDataFrame Info:")
        print(df.info())
        print("\nFirst few rows:")
        print(df.head())
        print("\nShape:", df.shape) 