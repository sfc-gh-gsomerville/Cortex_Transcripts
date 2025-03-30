from snowflake.snowpark import Session
import pandas as pd
from config import CONNECTION_PARAMETERS, SSO_CONNECTION_PARAMETERS

# Create a session using password authentication
def create_session():
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    print("Connected to Snowflake!")
    return session

# Create a session using SSO authentication
def create_session_sso():
    session = Session.builder.configs(SSO_CONNECTION_PARAMETERS).create()
    print("Connected to Snowflake using SSO!")
    return session

# Example usage
if __name__ == "__main__":
    try:
        # Connect to Snowflake (choose one of the methods below)
        # For password authentication:
        session = create_session()
        
        # For SSO authentication:
        # session = create_session_sso()
        
        # Example: Run a simple query
        df = session.sql("SELECT current_warehouse(), current_database(), current_schema()").collect()
        print("Current session info:")
        for row in df:
            print(row)
        
        # Explicitly use the database, schema, and warehouse from config
        session.sql(f"USE DATABASE {CONNECTION_PARAMETERS['database']}").collect()
        session.sql(f"USE SCHEMA {CONNECTION_PARAMETERS['database']}.{CONNECTION_PARAMETERS['schema']}").collect()
        session.sql(f"USE WAREHOUSE {CONNECTION_PARAMETERS['warehouse']}").collect()
        
        # Verify database and schema are set
        df = session.sql("SELECT current_warehouse(), current_database(), current_schema()").collect()
        print("\nUpdated session info:")
        for row in df:
            print(row)
        
        # Example: Query data and convert to pandas DataFrame
        pandas_df = session.sql("SELECT current_date() as today").to_pandas()
        print("\nToday's date from Snowflake:")
        print(pandas_df)
        
        # Example: Create a temporary table and query it
        session.sql("CREATE OR REPLACE TEMPORARY TABLE temp_test (id INT, name STRING)").collect()
        session.sql("INSERT INTO temp_test VALUES (1, 'Test1'), (2, 'Test2')").collect()
        
        results = session.sql("SELECT * FROM temp_test ORDER BY id").collect()
        print("\nResults from temporary table:")
        for row in results:
            print(row)
        
        # Close the session
        session.close()
        print("Session closed successfully")
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}") 
