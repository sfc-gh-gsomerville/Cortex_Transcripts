from snowflake.snowpark import Session
import pandas as pd
from config import CONNECTION_PARAMETERS

def create_session():
    # Create a copy of connection parameters and update database and schema
    cursor_demo_params = CONNECTION_PARAMETERS.copy()
    cursor_demo_params["database"] = "CURSOR_DEMO"
    cursor_demo_params["schema"] = "V1"
    
    session = Session.builder.configs(cursor_demo_params).create()
    print("Connected to Snowflake!")
    return session

def load_support_conversations():
    try:
        # Connect to Snowflake
        session = create_session()
        
        # Use the correct database and schema
        session.sql("USE DATABASE CURSOR_DEMO").collect()
        session.sql("USE SCHEMA CURSOR_DEMO.V1").collect()
        session.sql(f"USE WAREHOUSE {CONNECTION_PARAMETERS['warehouse']}").collect()
        
        # Verify current database and schema
        current = session.sql("SELECT current_database(), current_schema(), current_warehouse()").collect()
        print("Current session info:")
        for row in current:
            print(row)
        
        # Check if the table exists
        table_exists = session.sql("SHOW TABLES LIKE 'SUPPORT_CONVERSATIONS'").collect()
        if not table_exists:
            print("Table SUPPORT_CONVERSATIONS does not exist in CURSOR_DEMO.V1")
            session.close()
            return None
        
        # Query to get the first few rows to preview the structure
        print("\nTable preview:")
        preview = session.sql("SELECT * FROM SUPPORT_CONVERSATIONS LIMIT 5").collect()
        for row in preview:
            print(row)
        
        # Load the full table into a pandas DataFrame
        print("\nLoading full table into pandas DataFrame...")
        query = "SELECT * FROM SUPPORT_CONVERSATIONS"
        df = session.sql(query).to_pandas()
        
        # Print DataFrame info and summary
        print(f"\nLoaded {len(df)} rows into DataFrame")
        print("\nDataFrame info:")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Shape: {df.shape}")
        
        # Display the first few rows
        print("\nFirst 5 rows:")
        print(df.head())
        
        # Close the session
        session.close()
        print("\nSession closed successfully")
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    df = load_support_conversations()
    
    if df is not None:
        # Example operations with the DataFrame
        print("\nDataFrame summary statistics:")
        print(df.describe(include='all').transpose()) 
