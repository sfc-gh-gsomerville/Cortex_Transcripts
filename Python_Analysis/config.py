# Snowflake connection parameters
CONNECTION_PARAMETERS = {
    "account": "CGKCUIL-MUB55006",  # e.g., "xy12345.us-east-1" or "xy12345.region.aws" 
    "user": "gsomerville",
    "password": "<password>",  # Consider using environment variables for sensitive info
    "role": "ACCOUNTADMIN",          # e.g., "ACCOUNTADMIN"
    "warehouse": "COMPUTE_WH",  # Updated to use an existing warehouse
    "database": "CORTEX",
    "schema": "PUBLIC"         # e.g., "PUBLIC"
}

# Alternative connection method using SSO
SSO_CONNECTION_PARAMETERS = {
    "account": "CGKCUIL-MUB55006",
    "authenticator": "externalbrowser",  # For SSO authentication
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",  # Updated to use an existing warehouse
    "database": "CORTEX",
    "schema": "PUBLIC"
} 
