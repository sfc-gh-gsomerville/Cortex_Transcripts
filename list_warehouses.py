from snowflake.snowpark import Session
from config import CONNECTION_PARAMETERS

# Create a simplified connection config without database, schema and warehouse
connection_params = {k: v for k, v in CONNECTION_PARAMETERS.items() if k not in ['database', 'schema', 'warehouse']}

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
        
        # List all warehouses
        print("\nAvailable Warehouses:")
        warehouses = session.sql("SHOW WAREHOUSES").collect()
        for wh in warehouses:
            print(f"  - {wh['name']} (Size: {wh['size']})")
        
        # Close the session
        session.close()
        print("\nSession closed successfully")
        
    except Exception as e:
        print(f"Error: {e}") 