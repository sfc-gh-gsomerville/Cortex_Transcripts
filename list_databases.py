from snowflake.snowpark import Session
from config import CONNECTION_PARAMETERS

# Create a simplified connection config without database and schema
connection_params = {k: v for k, v in CONNECTION_PARAMETERS.items() if k not in ['database', 'schema']}

# Create a session
def create_session():
    session = Session.builder.configs(connection_params).create()
    print("Connected to Snowflake!")
    return session

# Main function
if __name__ == "__main__":
    try:
        # Connect to Snowflake
        session = create_session()
        
        # List all databases
        print("\nAvailable Databases:")
        databases = session.sql("SHOW DATABASES").collect()
        for db in databases:
            print(f"  - {db['name']}")
        
        # Try to access CORTEX database if it exists
        cortex_exists = any(db['name'].upper() == 'CORTEX' for db in databases)
        if cortex_exists:
            session.sql("USE DATABASE CORTEX").collect()
            
            # List all schemas in CORTEX
            print("\nSchemas in CORTEX database:")
            schemas = session.sql("SHOW SCHEMAS").collect()
            for schema in schemas:
                print(f"  - {schema['name']}")
        
        # Close the session
        session.close()
        print("\nSession closed successfully")
        
    except Exception as e:
        print(f"Error: {e}") 