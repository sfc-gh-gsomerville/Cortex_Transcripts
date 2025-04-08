# Explicitly set required package versions for Snowflake compatibility
# requirements.txt
# snowflake-snowpark-python==1.5.0
# pandas==1.5.3

import streamlit as st
import pandas as pd

# Set page configuration - MUST be the first Streamlit command
st.set_page_config(
    page_title="Column Case Debug",
    page_icon="🔍",
    layout="wide",
)

# Title and description
st.title("🔍 Snowflake Table Column Case Checker")
st.markdown("This tool helps diagnose case sensitivity issues with Snowflake tables")

# Initialize Snowflake session
session = st.connection('snowflake').session()

# Function to get database information
def get_snowflake_metadata():
    try:
        # Get current session info
        context_query = """
        SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE()
        """
        context_df = session.sql(context_query).to_pandas()
        if not context_df.empty:
            context = {
                'database': context_df.iloc[0, 0],
                'schema': context_df.iloc[0, 1],
                'role': context_df.iloc[0, 2],
                'warehouse': context_df.iloc[0, 3]
            }
        else:
            context = {}
        
        return context
    except Exception as e:
        st.error(f"Error getting Snowflake metadata: {e}")
        return {}

# Function to list tables in the current database/schema
def list_tables():
    try:
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error listing tables: {e}")
        return pd.DataFrame()

# Function to get column information for a specific table
def get_table_columns(table_name):
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, IS_NULLABLE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error getting columns for {table_name}: {e}")
        return pd.DataFrame()

# Function to check if a table exists
def check_table_exists(table_name):
    try:
        query = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{table_name}'
        AND TABLE_SCHEMA = CURRENT_SCHEMA()
        """
        result = session.sql(query).to_pandas()
        return result.iloc[0, 0] > 0
    except Exception as e:
        st.error(f"Error checking if table exists: {e}")
        return False

# Function to check for variations of a table name
def find_table_variants(base_name):
    try:
        query = f"""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE UPPER(TABLE_NAME) = UPPER('{base_name}')
        AND TABLE_SCHEMA = CURRENT_SCHEMA()
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error finding table variants: {e}")
        return pd.DataFrame()

# Function to sample data with explicit column names
def sample_data(table_name, columns, limit=5):
    try:
        if columns.empty:
            return pd.DataFrame()
            
        column_list = ", ".join(f'"{col}"' for col in columns['COLUMN_NAME'].tolist())
        query = f"""
        SELECT {column_list}
        FROM "{table_name}"
        LIMIT {limit}
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error sampling data: {e}")
        return pd.DataFrame()

# Get Snowflake metadata
context = get_snowflake_metadata()

# Display context information
st.subheader("Snowflake Session Information")
if context:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Database", context.get('database', 'Unknown'))
    col2.metric("Schema", context.get('schema', 'Unknown'))
    col3.metric("Role", context.get('role', 'Unknown'))
    col4.metric("Warehouse", context.get('warehouse', 'Unknown'))
else:
    st.warning("Could not retrieve Snowflake session information")

# Get available tables
tables_df = list_tables()

if not tables_df.empty:
    st.subheader("Available Tables")
    st.dataframe(tables_df, use_container_width=True)
    
    # Check specific table
    target_table = "TRANSCRIPT_ANALYSIS_RESULTS_FINAL"
    st.subheader(f"Checking Table: {target_table}")
    
    # Check if exactly this table exists
    if check_table_exists(target_table):
        st.success(f"Table '{target_table}' exists!")
    else:
        st.error(f"Table '{target_table}' not found with exact case.")
        
        # Look for case variations
        variants_df = find_table_variants(target_table)
        if not variants_df.empty:
            st.info(f"Found {len(variants_df)} case-insensitive match(es):")
            st.dataframe(variants_df, use_container_width=True)
            
            # Use the first variant for further analysis
            target_table = variants_df.iloc[0, 0]
            st.success(f"Using '{target_table}' for further analysis")
        else:
            st.error("No case-insensitive matches found")
            st.stop()
    
    # Get column information
    columns_df = get_table_columns(target_table)
    if not columns_df.empty:
        st.subheader(f"Columns in '{target_table}'")
        st.dataframe(columns_df, use_container_width=True)
        
        # Check for conversation_id specifically
        conv_id_variations = columns_df[columns_df['COLUMN_NAME'].str.upper() == 'CONVERSATION_ID']
        if not conv_id_variations.empty:
            st.success(f"Found CONVERSATION_ID column with case: {conv_id_variations['COLUMN_NAME'].iloc[0]}")
        else:
            st.error("CONVERSATION_ID column not found in any case variation")
        
        # Sample data
        st.subheader(f"Sample Data from '{target_table}'")
        sample_df = sample_data(target_table, columns_df)
        if not sample_df.empty:
            st.dataframe(sample_df, use_container_width=True)
        else:
            st.warning("Could not retrieve sample data")
            
        # Debug query with double quotes
        st.subheader("SQL Query with Double Quotes")
        column_list = ", ".join(f'"{col}"' for col in columns_df['COLUMN_NAME'].tolist()[:5])
        safe_query = f"""
        SELECT {column_list}
        FROM "{target_table}"
        LIMIT 5
        """
        st.code(safe_query, language="sql")
        
        # Try executing the query
        try:
            st.write("Executing query with explicit double quotes...")
            result = session.sql(safe_query).to_pandas()
            st.success("Query executed successfully!")
            st.dataframe(result, use_container_width=True)
        except Exception as e:
            st.error(f"Query execution failed: {e}")
    else:
        st.warning(f"Could not retrieve column information for '{target_table}'")
else:
    st.warning("No tables found in the current schema") 