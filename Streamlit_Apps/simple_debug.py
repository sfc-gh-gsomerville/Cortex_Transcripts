# Explicitly set required package versions for Snowflake compatibility
# requirements.txt
# snowflake-snowpark-python==1.5.0
# pandas==1.5.3

import streamlit as st
import pandas as pd
import traceback

# Set page config - must be first Streamlit command
st.set_page_config(
    page_title="Snowflake Connection Debug",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 Snowflake Connection Debugger")
st.markdown("This app helps diagnose connection and data access issues with the Snowflake database.")

# Initialize the Snowflake session with error handling
st.subheader("1. Connection Status")
try:
    session = st.connection('snowflake').session()
    st.success("✅ Successfully connected to Snowflake!")
    
    # Display session information
    session_info = session.sql("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").to_pandas()
    st.dataframe(session_info, use_container_width=True)
except Exception as e:
    st.error(f"❌ Failed to connect to Snowflake: {str(e)}")
    st.code(traceback.format_exc())
    st.stop()

# Check if the table exists
st.subheader("2. Table Verification")
try:
    # Use INFORMATION_SCHEMA to check if the table exists
    table_query = """
    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_NAME) = 'TRANSCRIPT_ANALYSIS_RESULTS_FINAL'
    """
    
    st.code(table_query, language="sql")
    table_exists_df = session.sql(table_query).to_pandas()
    
    if table_exists_df.empty:
        st.error("❌ Table 'TRANSCRIPT_ANALYSIS_RESULTS_FINAL' not found in the current database!")
        
        # List all tables to help find the correct one
        all_tables_query = """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        all_tables = session.sql(all_tables_query).to_pandas()
        
        st.markdown("#### Available tables in the current database:")
        st.dataframe(all_tables, use_container_width=True)
        st.stop()
    else:
        st.success(f"✅ Table found! Schema: {table_exists_df.iloc[0]['TABLE_SCHEMA']}, Name: {table_exists_df.iloc[0]['TABLE_NAME']}")
        table_name = table_exists_df.iloc[0]['TABLE_NAME']
        table_schema = table_exists_df.iloc[0]['TABLE_SCHEMA']
        
        # Display the exact table name for reference
        st.info(f"Exact table name: {table_name} (use this exact case in queries)")
        
        # Get fully qualified name
        fully_qualified_name = f'"{table_schema}"."{table_name}"'
        st.success(f"Fully qualified name: {fully_qualified_name}")
except Exception as e:
    st.error(f"❌ Error checking table existence: {str(e)}")
    st.code(traceback.format_exc())
    st.stop()

# Get schema information
st.subheader("3. Table Schema")
try:
    # Use INFORMATION_SCHEMA to get column information
    schema_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE UPPER(TABLE_NAME) = 'TRANSCRIPT_ANALYSIS_RESULTS_FINAL'
    ORDER BY ORDINAL_POSITION
    """
    
    st.code(schema_query, language="sql")
    schema_df = session.sql(schema_query).to_pandas()
    
    if schema_df.empty:
        st.error("❌ Could not retrieve schema information!")
        st.stop()
    else:
        st.success(f"✅ Retrieved schema with {len(schema_df)} columns")
        st.dataframe(schema_df, use_container_width=True)
        
        # Check specifically for CONVERSATION_ID column
        conversation_id_cols = schema_df[schema_df['COLUMN_NAME'].str.upper() == 'CONVERSATION_ID']
        if not conversation_id_cols.empty:
            exact_name = conversation_id_cols.iloc[0]['COLUMN_NAME']
            st.success(f"✅ CONVERSATION_ID column found as '{exact_name}'")
        else:
            st.error("❌ CONVERSATION_ID column not found in schema!")
            
            # Find columns with "ID" in their name
            id_columns = schema_df[schema_df['COLUMN_NAME'].str.upper().str.contains('ID')]
            if not id_columns.empty:
                st.info("Columns with 'ID' in their name:")
                st.dataframe(id_columns[['COLUMN_NAME', 'DATA_TYPE']], use_container_width=True)
except Exception as e:
    st.error(f"❌ Error retrieving schema: {str(e)}")
    st.code(traceback.format_exc())

# Try to fetch some sample data
st.subheader("4. Sample Data")
try:
    # First, let's find out the exact column name for CONVERSATION_ID
    conv_id_col = None
    if 'conversation_id_cols' in locals() and not conversation_id_cols.empty:
        conv_id_col = conversation_id_cols.iloc[0]['COLUMN_NAME']
    
    # Get column names with exact case
    column_names = schema_df['COLUMN_NAME'].tolist() if 'schema_df' in locals() and not schema_df.empty else []
    
    # Create a query with explicit column names (exact case)
    if column_names:
        columns_str = ', '.join([f'"{col}"' for col in column_names[:10]])  # First 10 columns to keep it manageable
        query = f"""
        SELECT {columns_str}
        FROM {fully_qualified_name}
        LIMIT 5
        """
    else:
        # Fallback query if we couldn't get column names
        query = f"""
        SELECT *
        FROM {fully_qualified_name}
        LIMIT 5
        """
    
    st.code(query, language="sql")
    
    # Execute the query
    sample_data = session.sql(query).to_pandas()
    
    if sample_data.empty:
        st.warning("⚠️ No data returned from the sample query!")
    else:
        st.success(f"✅ Successfully retrieved {len(sample_data)} rows of sample data")
        st.dataframe(sample_data, use_container_width=True)
        
        # Check for the conversation_id column in the sample data
        if conv_id_col and conv_id_col in sample_data.columns:
            st.success(f"✅ '{conv_id_col}' column exists in the sample data")
        elif conv_id_col:
            st.error(f"❌ '{conv_id_col}' column exists in schema but not in the sample data!")
except Exception as e:
    st.error(f"❌ Error fetching sample data: {str(e)}")
    st.code(traceback.format_exc())

# Provide recommendations
st.subheader("5. Recommendations")
st.markdown("""
### Troubleshooting Steps

1. **Check Permissions**
   - Ensure your role has SELECT access to the table
   - If you're getting errors about missing columns, check column-level permissions

2. **Case Sensitivity**
   - Snowflake is case-sensitive with unquoted identifiers
   - Always use double quotes for table and column names: `"COLUMN_NAME"`
   - Update your dashboard code to match the exact case of columns

3. **Use Fully Qualified Names**
   - When accessing tables, use fully qualified names: `"DATABASE"."SCHEMA"."TABLE"`
   - This avoids issues with the current database or schema settings

4. **Fixing the Dashboard**
   - Update the main dashboard to use the correct column names with proper case
   - Use this exact column name for conversation_id: "{conv_id_col if 'conv_id_col' in locals() and conv_id_col else 'UNKNOWN'}"
   - Consider explicitly listing columns in your SQL query rather than using SELECT *
""")

st.markdown("---")
st.markdown("### Example Query with Fully Qualified Name and Explicit Columns")

if 'fully_qualified_name' in locals() and 'schema_df' in locals() and not schema_df.empty:
    sample_cols = schema_df['COLUMN_NAME'].tolist()[:5]  # First 5 columns as example
    example_cols_str = ', '.join([f'"{col}"' for col in sample_cols])
    example_query = f"""
    SELECT {example_cols_str}
    FROM {fully_qualified_name}
    LIMIT 10
    """
    st.code(example_query, language="sql") 