# Explicitly set required package versions for Snowflake compatibility
# requirements.txt
# snowflake-snowpark-python==1.5.0
# numpy==1.24.3 
# plotly==5.14.1
# pandas==1.5.3

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Set page configuration - MUST be the first Streamlit command
st.set_page_config(
    page_title="Transcript Analysis",
    page_icon="📊",
    layout="wide",
)

# Initialize Snowflake session
session = st.connection('snowflake').session()

# Title and description
st.title("📞 Customer Support Transcript Analysis")
st.markdown("Analysis of customer support transcripts")

# Get the available columns to work with
@st.cache_data(ttl=300)
def get_table_columns():
    try:
        # Get column information from INFORMATION_SCHEMA
        query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'TRANSCRIPT_ANALYSIS_RESULTS_FINAL'
        """
        columns_df = session.sql(query).to_pandas()
        if len(columns_df) > 0:
            # Convert to lowercase for case-insensitive comparison
            available_columns = [col.lower() for col in columns_df['COLUMN_NAME']]
            return available_columns
        else:
            return []
    except Exception as e:
        st.sidebar.error(f"Error getting columns: {e}")
        return []

# Function to load data with columns that exist
@st.cache_data(ttl=300)
def load_data(available_columns=[]):
    try:
        # Build a query using only available columns
        if available_columns:
            # Get column list with proper casing from the source
            column_str = ", ".join(available_columns)
            query = f"""
            SELECT {column_str}
            FROM TRANSCRIPT_ANALYSIS_RESULTS_FINAL
            """
        else:
            # Fallback to SELECT * if we couldn't determine columns
            query = "SELECT * FROM TRANSCRIPT_ANALYSIS_RESULTS_FINAL"
            
        df = session.sql(query).to_pandas()
        
        # Convert columns to lowercase for easier access
        df.columns = [col.lower() for col in df.columns]
        
        # Convert date columns
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        
        # Convert numeric columns
        if 'service_rating' in df.columns:
            df['service_rating'] = pd.to_numeric(df['service_rating'], errors='coerce')
        if 'sentiment_score' in df.columns:
            df['sentiment_score'] = pd.to_numeric(df['sentiment_score'], errors='coerce')
            
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        # Return an empty DataFrame
        return pd.DataFrame()

# Get available columns
available_columns = get_table_columns()

# Load the data with verified columns
df = load_data(available_columns)

if df.empty:
    st.error("Unable to load data from the TRANSCRIPT_ANALYSIS_RESULTS_FINAL table.")
    st.info("Please verify that the table exists and you have permission to access it.")
    
    # Show debug information
    st.expander("Debug Information").write(
        f"Available columns found: {', '.join(available_columns) if available_columns else 'None'}"
    )
    st.stop()

# Display basic information
st.subheader("Data Overview")
st.write(f"Total records: {len(df)}")
st.write(f"Columns available: {', '.join(df.columns.tolist())}")

# Create tabs for different analyses
tab1, tab2 = st.tabs(["Data Exploration", "Visualization"])

# Tab 1: Data exploration
with tab1:
    # Display a sample of the data
    st.subheader("Sample Data")
    st.dataframe(df.head(10), use_container_width=True)
    
    # Column selection for detailed view
    if len(df.columns) > 0:
        selected_column = st.selectbox("Select a column to analyze", sorted(df.columns))
        
        # Only proceed if a column is selected and exists
        if selected_column and selected_column in df.columns:
            st.subheader(f"Analysis of '{selected_column}'")
            
            # Get column data type
            dtype = df[selected_column].dtype
            st.write(f"Data type: {dtype}")
            
            # For numeric columns
            if pd.api.types.is_numeric_dtype(df[selected_column]):
                # Show basic statistics
                stats = df[selected_column].describe()
                st.dataframe(stats, use_container_width=True)
                
                # Create a histogram
                fig = px.histogram(
                    df, 
                    x=selected_column,
                    title=f"Distribution of {selected_column}",
                    color_discrete_sequence=['#1f77b4']
                )
                st.plotly_chart(fig, use_container_width=True)
                
            # For datetime columns
            elif pd.api.types.is_datetime64_any_dtype(df[selected_column]):
                # Show time range
                min_date = df[selected_column].min()
                max_date = df[selected_column].max()
                st.write(f"Date range: {min_date} to {max_date}")
                
                # Group by day and count
                if not df[selected_column].isna().all():
                    date_counts = df.groupby(df[selected_column].dt.date).size().reset_index()
                    date_counts.columns = ['Date', 'Count']
                    
                    fig = px.line(
                        date_counts, 
                        x='Date', 
                        y='Count',
                        title=f"Counts by date ({selected_column})"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
            # For categorical/string columns
            else:
                # Get value counts
                value_counts = df[selected_column].value_counts().reset_index()
                value_counts.columns = [selected_column, 'Count']
                
                # Limit to top values if there are many
                if len(value_counts) > 20:
                    st.write(f"Showing top 20 values out of {len(value_counts)} unique values")
                    value_counts = value_counts.head(20)
                
                # Show value counts
                st.dataframe(value_counts, use_container_width=True)
                
                # Create a bar chart
                fig = px.bar(
                    value_counts, 
                    x=selected_column, 
                    y='Count',
                    title=f"Value counts for {selected_column}"
                )
                st.plotly_chart(fig, use_container_width=True)

# Tab 2: Visualization
with tab2:
    st.subheader("Create Visualizations")
    
    # Select visualization type
    viz_type = st.selectbox(
        "Select visualization type",
        ["Bar Chart", "Pie Chart", "Line Chart", "Scatter Plot"]
    )
    
    # Only numeric and datetime columns for x and y axes
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    categorical_cols = [col for col in df.columns if col not in numeric_cols and col not in datetime_cols]
    
    if viz_type == "Bar Chart":
        # For bar charts
        x_col = st.selectbox("Select X-axis (categorical)", categorical_cols if categorical_cols else ["No categorical columns found"])
        y_col = st.selectbox("Select Y-axis (numeric)", numeric_cols if numeric_cols else ["No numeric columns found"])
        
        if categorical_cols and numeric_cols:
            agg_func = st.selectbox("Aggregation function", ["mean", "sum", "count", "min", "max"])
            
            # Group by x column and aggregate y column
            grouped_data = df.groupby(x_col)[y_col].agg(agg_func).reset_index()
            
            fig = px.bar(
                grouped_data, 
                x=x_col, 
                y=y_col,
                title=f"{agg_func.capitalize()} of {y_col} by {x_col}",
                color=y_col,
                color_continuous_scale="Blues"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Insufficient column types for this visualization")
    
    elif viz_type == "Pie Chart":
        # For pie charts
        cat_col = st.selectbox("Select category column", categorical_cols if categorical_cols else ["No categorical columns found"])
        
        if categorical_cols:
            # Get value counts
            value_counts = df[cat_col].value_counts().reset_index()
            value_counts.columns = [cat_col, 'Count']
            
            # Limit to top values if there are many
            if len(value_counts) > 10:
                st.info(f"Showing top 10 values out of {len(value_counts)} unique values")
                value_counts = value_counts.head(10)
            
            fig = px.pie(
                value_counts, 
                values='Count', 
                names=cat_col,
                title=f"Distribution of {cat_col}"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No categorical columns found for this visualization")
    
    elif viz_type == "Line Chart":
        # For line charts
        if datetime_cols:
            time_col = st.selectbox("Select time column", datetime_cols)
            value_col = st.selectbox("Select value column", numeric_cols if numeric_cols else ["No numeric columns found"])
            
            if numeric_cols:
                # Group by day and calculate mean
                time_data = df.groupby(df[time_col].dt.date)[value_col].mean().reset_index()
                time_data.columns = ['Date', value_col]
                
                fig = px.line(
                    time_data, 
                    x='Date', 
                    y=value_col,
                    title=f"{value_col} over time"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No numeric columns found for this visualization")
        else:
            st.warning("No date/time columns found for this visualization")
    
    elif viz_type == "Scatter Plot":
        # For scatter plots
        x_col = st.selectbox("Select X-axis (numeric)", numeric_cols if numeric_cols else ["No numeric columns found"])
        y_col = st.selectbox("Select Y-axis (numeric)", 
                            [col for col in numeric_cols if col != x_col] if len(numeric_cols) > 1 else numeric_cols,
                            index=min(1, len(numeric_cols)-1) if len(numeric_cols) > 1 else 0)
        
        if len(numeric_cols) >= 2:
            # Optional color by category
            color_options = ["None"] + categorical_cols
            color_col = st.selectbox("Color by (optional)", color_options)
            
            color_param = color_col if color_col != "None" else None
            
            fig = px.scatter(
                df, 
                x=x_col, 
                y=y_col,
                color=color_param,
                title=f"Scatter plot of {y_col} vs {x_col}",
                trendline="ols" if st.checkbox("Show trend line") else None
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Need at least two numeric columns for this visualization") 