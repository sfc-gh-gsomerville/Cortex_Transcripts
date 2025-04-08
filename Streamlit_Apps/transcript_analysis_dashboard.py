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
import numpy as np
import re
import time
import traceback

# Set page configuration - MUST be the first Streamlit command
st.set_page_config(
    page_title="Transcript Analysis Dashboard",
    page_icon="📊",
    layout="wide",
)

# Initialize session
session = st.connection('snowflake').session()

# Title and description
st.title("📞 Customer Support Transcript Analysis")
st.markdown("Comprehensive analysis of customer support transcripts to identify trends and improvement opportunities.")

# Debug information in sidebar
with st.sidebar.expander("Debug Information", expanded=False):
    st.write("This section shows information about the data being loaded")
    
    # Show session info
    try:
        session_info = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()").to_pandas()
        st.write("Session Information:")
        st.dataframe(session_info)
    except Exception as e:
        st.error(f"Error getting session info: {e}")

# Load data with improved error handling
def load_data():
    try:
        # Initialize connection
        session = st.connection('snowflake').session()
        
        # Get the exact column names from schema
        schema_query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_NAME) = 'TRANSCRIPT_ANALYSIS_RESULTS_FINAL'
        ORDER BY ORDINAL_POSITION
        """
        
        schema_df = session.sql(schema_query).to_pandas()
        
        if schema_df.empty:
            st.warning("Could not retrieve schema information for table. Check if table exists.")
            return pd.DataFrame(), {}
        
        # Create a mapping of lowercase column names to actual column names
        column_mapping = {col.lower(): col for col in schema_df['COLUMN_NAME']}
        
        # Check if conversation_id column exists and get its exact case
        conv_id_col = None
        for col in schema_df['COLUMN_NAME']:
            if col.upper() == 'CONVERSATION_ID':
                conv_id_col = col
                break
        
        if not conv_id_col:
            st.error("CONVERSATION_ID column not found in schema! Check the table structure.")
            # Find columns with "ID" in their name as possible alternatives
            id_columns = [col for col in schema_df['COLUMN_NAME'] if 'ID' in col.upper()]
            if id_columns:
                st.info(f"Columns with 'ID' in their name: {', '.join(id_columns)}")
            return pd.DataFrame(), column_mapping
        
        # Use double quotes around table name and all columns
        query = f"""
        SELECT *
        FROM "TRANSCRIPT_ANALYSIS_RESULTS_FINAL"
        ORDER BY "START_TIME" DESC
        """
        
        # Query data
        df = session.sql(query).to_pandas()
        
        if df.empty:
            st.warning("No data available from TRANSCRIPT_ANALYSIS_RESULTS_FINAL table.")
            return pd.DataFrame(), column_mapping
        
        # Add debug info to sidebar
        with st.sidebar.expander("Debug Info - Column Names"):
            st.write("Original column names in the table:")
            st.dataframe(schema_df)
            st.write("Lowercase to original mapping:")
            st.json(column_mapping)
        
        # Verify conversation_id exists in the data
        if conv_id_col in df.columns:
            st.sidebar.success(f"✅ Successfully loaded data with {conv_id_col} column")
        else:
            st.sidebar.error(f"❌ Error: {conv_id_col} not in returned data columns!")
            st.sidebar.write("Available columns:", df.columns.tolist())
        
        # Process date columns
        date_cols = ['start_time', 'end_time']
        for col in date_cols:
            col_exact = next((c for c in df.columns if c.lower() == col.lower()), None)
            if col_exact and col_exact in df.columns:
                try:
                    df[col_exact] = pd.to_datetime(df[col_exact], errors='coerce')
                except Exception as e:
                    st.sidebar.warning(f"Could not convert {col_exact} to datetime: {str(e)}")
        
        # Process numeric columns with better error handling
        numeric_cols = ['service_rating', 'sentiment_score']
        for col in numeric_cols:
            col_exact = next((c for c in df.columns if c.lower() == col.lower()), None)
            if col_exact and col_exact in df.columns:
                try:
                    # Try to convert non-numeric strings to numeric safely
                    if df[col_exact].dtype == 'object':
                        # First attempt: Extract numeric parts from strings if possible
                        try:
                            # For service_rating, extract first digit (assuming it's a 0-10 rating)
                            if col.lower() == 'service_rating':
                                # Try to extract the first number from the string
                                df[col_exact] = df[col_exact].astype(str).str.extract(r'(\d+)').astype(float)
                                st.sidebar.info(f"Extracted numeric values from {col_exact}")
                            else:
                                # For other columns, use pd.to_numeric with errors='coerce'
                                df[col_exact] = pd.to_numeric(df[col_exact], errors='coerce')
                        except Exception as e:
                            st.sidebar.warning(f"Could not extract numeric values from {col_exact}: {str(e)}")
                            # Fallback: replace with NaN
                            df[col_exact] = np.nan
                    else:
                        # If already numeric type, just ensure it's float
                        df[col_exact] = pd.to_numeric(df[col_exact], errors='coerce')
                except Exception as e:
                    st.sidebar.warning(f"Error converting {col_exact} to numeric: {str(e)}")
                    df[col_exact] = np.nan
        
        return df, column_mapping
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.code(traceback.format_exc())
        return pd.DataFrame(), {}

# Load data
data, column_mapping = load_data()

# If we have data, proceed with the dashboard
if not data.empty:
    # Get exact column names using the mapping
    def get_col(col_name):
        # Look for exact match first, then case-insensitive
        if col_name in data.columns:
            return col_name
        
        # Try using the mapping
        if col_name.lower() in column_mapping:
            return column_mapping[col_name.lower()]
        
        # As a last resort, try case-insensitive search
        for col in data.columns:
            if col.lower() == col_name.lower():
                return col
        
        return None

    # Find conversation_id column
    conv_id_col = get_col("conversation_id")
    if not conv_id_col:
        st.error("Could not find conversation_id column in the data.")
        st.stop()
    
    # Find other columns using the mapping
    start_time_col = get_col("start_time")
    source_col = get_col("source")
    device_col = get_col("device_category")
    service_rating_col = get_col("service_rating")
    sentiment_col = get_col("sentiment_score")
    resolution_col = get_col("resolution")
    duration_col = get_col("duration_minutes")
    
    # Display debug information
    with st.sidebar.expander("Column Name Mapping"):
        st.write({
            "conversation_id": conv_id_col,
            "start_time": start_time_col,
            "source": source_col,
            "device_category": device_col,
            "service_rating": service_rating_col,
            "sentiment_score": sentiment_col,
            "resolution": resolution_col,
            "duration_minutes": duration_col
        })
    
    # Set up date filter in sidebar
    st.sidebar.title("Filters")
    if start_time_col and start_time_col in data.columns:
        min_date = data[start_time_col].min().date() if not data.empty else datetime.now().date() - timedelta(days=30)
        max_date = data[start_time_col].max().date() if not data.empty else datetime.now().date()
        
        start_date = st.sidebar.date_input("Start date", min_date, min_value=min_date, max_value=max_date)
        end_date = st.sidebar.date_input("End date", max_date, min_value=min_date, max_value=max_date)
        
        # Filter data based on date
        filtered_data = data.copy()
        if start_time_col in filtered_data.columns:
            filtered_data = filtered_data[
                (filtered_data[start_time_col].dt.date >= start_date) &
                (filtered_data[start_time_col].dt.date <= end_date)
            ]
    else:
        filtered_data = data.copy()
        st.sidebar.warning("Date filtering not available: start_time column not found")
    
    # Source filter
    if source_col and source_col in filtered_data.columns:
        source_options = ["All"] + sorted(filtered_data[source_col].unique().tolist())
        selected_source = st.sidebar.selectbox("Source", source_options)
        
        if selected_source != "All":
            filtered_data = filtered_data[filtered_data[source_col] == selected_source]
    
    # Device category filter
    if device_col and device_col in filtered_data.columns:
        device_options = ["All"] + sorted(filtered_data[device_col].unique().tolist())
        selected_device = st.sidebar.selectbox("Device Category", device_options)
        
        if selected_device != "All":
            filtered_data = filtered_data[filtered_data[device_col] == selected_device]
    
    # Main dashboard content
    st.title("📊 Transcript Analysis Dashboard")
    st.write(f"Data from {start_date} to {end_date}" if start_time_col in filtered_data.columns else "Full dataset")
    
    # Create tabs for different analyses
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Overview", "Resolution Analysis", "Sentiment Analysis", "Time Analysis", "Agent Performance"
    ])
    
    # Tab 1: Overview
    with tab1:
        st.header("Overview")
        
        # Calculate metrics
        total_transcripts = len(filtered_data)
        
        # Check if columns exist and contain valid numeric data before calculations
        avg_rating = None
        if service_rating_col and service_rating_col in filtered_data.columns:
            try:
                # Make sure we're working with numeric values
                ratings = pd.to_numeric(filtered_data[service_rating_col], errors='coerce')
                if not ratings.isna().all():  # Only calculate if we have some non-NaN values
                    avg_rating = ratings.mean()
            except Exception as e:
                st.sidebar.warning(f"Error calculating average rating: {str(e)}")
        
        avg_sentiment = None
        if sentiment_col and sentiment_col in filtered_data.columns:
            try:
                # Make sure we're working with numeric values
                sentiments = pd.to_numeric(filtered_data[sentiment_col], errors='coerce')
                if not sentiments.isna().all():  # Only calculate if we have some non-NaN values
                    avg_sentiment = sentiments.mean()
            except Exception as e:
                st.sidebar.warning(f"Error calculating average sentiment: {str(e)}")
        
        avg_duration = None
        if duration_col and duration_col in filtered_data.columns:
            try:
                # Make sure we're working with numeric values
                durations = pd.to_numeric(filtered_data[duration_col], errors='coerce')
                if not durations.isna().all():  # Only calculate if we have some non-NaN values
                    avg_duration = durations.mean()
            except Exception as e:
                st.sidebar.warning(f"Error calculating average duration: {str(e)}")
        
        # Create columns for metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Transcripts", f"{total_transcripts:,}")
        
        with col2:
            if avg_rating is not None:
                st.metric("Avg Service Rating", f"{avg_rating:.2f}")
            else:
                st.metric("Avg Service Rating", "N/A")
        
        with col3:
            if avg_sentiment is not None:
                st.metric("Avg Sentiment Score", f"{avg_sentiment:.2f}")
            else:
                st.metric("Avg Sentiment Score", "N/A")
        
        with col4:
            if avg_duration is not None:
                st.metric("Avg Duration (min)", f"{avg_duration:.2f}")
            else:
                st.metric("Avg Duration (min)", "N/A")
        
        # Create columns for visualizations
        col_left, col_right = st.columns(2)
                
        # Device Categories
        with col_left:
            if device_col and device_col in filtered_data.columns:
                st.subheader("Device Categories")
                device_counts = filtered_data[device_col].value_counts().reset_index()
                device_counts.columns = ['Device Category', 'Count']
                
                fig = px.pie(
                    device_counts, 
                    values='Count', 
                    names='Device Category',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set2
                )
                fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True, key="device_categories_pie")
            else:
                st.warning("Device category information is not available")
        
        # Modify the sentiment distribution logic in the right column to safely handle non-numeric values
        with col_right:
            # Sentiment Distribution
            st.subheader("Sentiment Distribution")
            if sentiment_col and sentiment_col in filtered_data.columns:
                try:
                    # Make sure we're working with numeric values
                    sentiments = pd.to_numeric(filtered_data[sentiment_col], errors='coerce')
                    
                    # Check if we have enough valid values
                    if not sentiments.isna().all() and sentiments.count() > 0:
                        # Create bins for sentiment scores
                        filtered_data['sentiment_category'] = pd.cut(
                            sentiments, 
                            bins=[-1, -0.5, 0, 0.5, 1], 
                            labels=['Very Negative', 'Negative', 'Positive', 'Very Positive']
                        )
                        
                        sentiment_counts = filtered_data['sentiment_category'].value_counts().reset_index()
                        sentiment_counts.columns = ['Sentiment', 'Count']
                        sentiment_counts = sentiment_counts.sort_values('Sentiment')
                        
                        # Only create chart if we have data
                        if not sentiment_counts.empty:
                            fig = px.bar(
                                sentiment_counts, 
                                x='Sentiment', 
                                y='Count',
                                color='Sentiment',
                                color_discrete_sequence=px.colors.sequential.RdBu,
                                text_auto=True
                            )
                            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
                            st.plotly_chart(fig, use_container_width=True, key="overview_sentiment_distribution_bar")
                        else:
                            st.warning("No valid sentiment categories after binning")
                    else:
                        st.warning("Sentiment scores are all missing or invalid")
                except Exception as e:
                    st.error(f"Error processing sentiment data: {str(e)}")
            else:
                st.warning("Sentiment data is not available")
    
    # Tab 2: Resolution Analysis
    with tab2:
        st.header("Resolution Analysis")
        
        if resolution_col and resolution_col in filtered_data.columns:
            resolution_counts = filtered_data[resolution_col].value_counts().reset_index()
            resolution_counts.columns = ['Resolution', 'Count']
            
            fig = px.bar(
                resolution_counts, 
                x='Resolution', 
                y='Count',
                color='Resolution',
                text_auto=True
            )
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True, key="resolution_bar")
            
            # Resolution by Device (if both columns exist)
            if device_col and device_col in filtered_data.columns:
                st.subheader("Resolution by Device Category")
                
                resolution_device = filtered_data.groupby([resolution_col, device_col]).size().reset_index(name='Count')
                
                fig = px.bar(
                    resolution_device, 
                    x=resolution_col, 
                    y='Count',
                    color=device_col,
                    barmode='group',
                    title='Resolution by Device Category'
                )
                fig.update_layout(margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True, key="resolution_by_device_bar")
        else:
            st.warning("Resolution data is not available")
    
    # Tab 3: Sentiment Analysis
    with tab3:
        st.header("Sentiment Analysis")
        
        if sentiment_col and sentiment_col in filtered_data.columns:
            # Create bins for sentiment scores
            filtered_data['sentiment_category'] = pd.cut(
                filtered_data[sentiment_col], 
                bins=[-1, -0.5, 0, 0.5, 1], 
                labels=['Very Negative', 'Negative', 'Positive', 'Very Positive']
            )
            
            sentiment_counts = filtered_data['sentiment_category'].value_counts().reset_index()
            sentiment_counts.columns = ['Sentiment', 'Count']
            sentiment_counts = sentiment_counts.sort_values('Sentiment')
            
            fig = px.bar(
                sentiment_counts, 
                x='Sentiment', 
                y='Count',
                color='Sentiment',
                color_discrete_sequence=px.colors.sequential.RdBu,
                text_auto=True
            )
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True, key="sentiment_tab_distribution_bar")
        else:
            st.warning("Sentiment data is not available")
    
    # Tab 4: Time Analysis
    with tab4:
        st.header("Time Analysis")
        
        if start_time_col and start_time_col in filtered_data.columns:
            # Create date column for grouping
            filtered_data['date'] = filtered_data[start_time_col].dt.date
            
            # Group by date
            daily_counts = filtered_data.groupby('date').size().reset_index(name='count')
            
            # Create time series chart
            fig = px.line(
                daily_counts, 
                x='date', 
                y='count',
                markers=True,
                labels={'count': 'Number of Transcripts', 'date': 'Date'},
                title='Daily Transcript Volume'
            )
            fig.update_layout(margin=dict(t=40, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True, key="daily_transcript_line")
            
            # If sentiment data is available, add sentiment time series
            if sentiment_col and sentiment_col in filtered_data.columns:
                daily_sentiment = filtered_data.groupby('date')[sentiment_col].mean().reset_index()
                
                fig = px.line(
                    daily_sentiment, 
                    x='date', 
                    y=sentiment_col,
                    markers=True,
                    labels={sentiment_col: 'Average Sentiment', 'date': 'Date'},
                    title='Daily Average Sentiment'
                )
                fig.update_layout(margin=dict(t=40, b=0, l=0, r=0))
                st.plotly_chart(fig, use_container_width=True, key="daily_sentiment_line")
        else:
            st.warning("Time series analysis not available: start_time column not found")
    
    # Tab 5: Agent Performance
    with tab5:
        st.header("Agent Performance Analysis")
        
        # Find agent_name column
        agent_name_col = get_col("agent_name")
        
        if agent_name_col and agent_name_col in filtered_data.columns and not filtered_data[agent_name_col].isna().all():
            # Calculate metrics by agent
            agents_df = pd.DataFrame()
            
            # Count conversations by agent
            conversation_counts = filtered_data[agent_name_col].value_counts().reset_index()
            conversation_counts.columns = ['agent_name', 'conversation_count']
            agents_df = conversation_counts
            
            # Calculate average service rating by agent if available
            if service_rating_col and service_rating_col in filtered_data.columns:
                avg_ratings = filtered_data.groupby(agent_name_col)[service_rating_col].mean().reset_index()
                avg_ratings.columns = ['agent_name', 'avg_service_rating']
                agents_df = agents_df.merge(avg_ratings, on='agent_name', how='left')
            
            # Calculate average sentiment score by agent if available
            if sentiment_col and sentiment_col in filtered_data.columns:
                avg_sentiment = filtered_data.groupby(agent_name_col)[sentiment_col].mean().reset_index()
                avg_sentiment.columns = ['agent_name', 'avg_sentiment_score']
                agents_df = agents_df.merge(avg_sentiment, on='agent_name', how='left')
            
            # Calculate resolution rate by agent if available
            if resolution_col and resolution_col in filtered_data.columns:
                # Create a crosstab of agent vs resolution category
                agent_resolution_breakdown = pd.crosstab(
                    filtered_data[agent_name_col], 
                    filtered_data[resolution_col], 
                    normalize='index'
                ) * 100
                
                # Round for better display
                for col in agent_resolution_breakdown.columns:
                    agent_resolution_breakdown[col] = agent_resolution_breakdown[col].round(1)
                
                # Reset index to make agent_name a column
                agent_resolution_breakdown = agent_resolution_breakdown.reset_index()
                
                # Ensure the column name matches
                if agent_name_col != 'agent_name' and 'agent_name' not in agent_resolution_breakdown.columns:
                    agent_resolution_breakdown = agent_resolution_breakdown.rename(columns={agent_name_col: 'agent_name'})
                
                # Make sure agents_df also has 'agent_name' column
                if agent_name_col != 'agent_name' and 'agent_name' not in agents_df.columns:
                    agents_df = agents_df.rename(columns={agent_name_col: 'agent_name'})
                
                # Merge with agents_df to ensure we have all agents
                agents_df = agents_df.merge(
                    agent_resolution_breakdown, 
                    on='agent_name', 
                    how='left'
                )
                
                # Fill NaN values
                resolution_categories = ['Resolved', 'Partial', 'Unresolved']
                for col in resolution_categories:
                    if col in agents_df.columns:
                        agents_df[col] = agents_df[col].fillna(0)
            
            # Calculate average conversation duration by agent if available
            if duration_col and duration_col in filtered_data.columns:
                avg_duration = filtered_data.groupby(agent_name_col)[duration_col].mean().reset_index()
                avg_duration.columns = ['agent_name', 'avg_duration']
                agents_df = agents_df.merge(avg_duration, on='agent_name', how='left')
            
            # Display the agent performance table
            st.subheader("Agent Performance Metrics")
            
            # Define columns to display based on what's available
            display_cols = ['agent_name', 'conversation_count']
            
            # Only add columns if they exist
            if 'avg_duration' in agents_df.columns:
                display_cols.append('avg_duration')
            if 'avg_service_rating' in agents_df.columns:
                display_cols.append('avg_service_rating')
            
            # Add resolution category columns if they exist
            resolution_category_cols = []
            for col in ['Resolved', 'Partial', 'Unresolved']:
                if col in agents_df.columns:
                    display_cols.append(col)
                    resolution_category_cols.append(col)
                    
            if 'avg_sentiment_score' in agents_df.columns:
                display_cols.append('avg_sentiment_score')
            
            # Format the dataframe for display (ensure all requested columns exist)
            available_cols = [col for col in display_cols if col in agents_df.columns]
            formatted_df = agents_df[available_cols].copy()
            
            # Add % to resolution rate and sentiment category columns
            for col in resolution_category_cols:
                if col in formatted_df.columns:
                    formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:.1f}%")
            
            # Add % to sentiment category columns
            for col in ['Positive', 'Neutral', 'Negative', 'Very Positive', 'Very Negative']:
                if col in formatted_df.columns:
                    formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:.1f}%")
            
            # Rename columns for better display
            column_renames = {
                'agent_name': 'Agent',
                'conversation_count': 'Conversations',
                'avg_duration': 'Avg Duration (min)',
                'avg_service_rating': 'Avg Rating (/10)',
                'Resolved': 'Resolved %',
                'Partial': 'Partial %',
                'Unresolved': 'Unresolved %',
                'resolution_rate': 'Resolution Rate',
                'avg_sentiment_score': 'Avg Sentiment',
                'Positive': 'Positive %',
                'Neutral': 'Neutral %',
                'Negative': 'Negative %',
                'Very Positive': 'Very Positive %',
                'Very Negative': 'Very Negative %'
            }
            
            # Only rename columns that exist in the dataframe
            rename_dict = {col: column_renames[col] for col in formatted_df.columns if col in column_renames}
            formatted_df = formatted_df.rename(columns=rename_dict)
            
            # Display the table
            st.dataframe(formatted_df)
            
            # Create visualizations for performance metrics if we have multiple agents
            if len(agents_df) > 1:
                st.subheader("Performance Visualizations")
                
                # Service Rating Comparison
                if 'avg_service_rating' in agents_df.columns:
                    fig = px.bar(
                        agents_df.sort_values('avg_service_rating', ascending=False),
                        x='agent_name',
                        y='avg_service_rating',
                        title="Average Service Rating by Agent",
                        labels={'agent_name': 'Agent', 'avg_service_rating': 'Average Rating (0-10)'},
                        color='avg_service_rating',
                        color_continuous_scale='RdYlGn'
                    )
                    fig.update_layout(yaxis_range=[0, 10])
                    st.plotly_chart(fig, use_container_width=True, key="agent_rating_bar")
                
                # Resolution Rate Comparison
                if 'Resolved' in agents_df.columns:
                    st.subheader("Resolution Breakdown by Agent")
                    
                    # Prepare data for stacked bar chart
                    resolution_data = []
                    for idx, row in agents_df.iterrows():
                        agent = row['agent_name']
                        for cat in ['Resolved', 'Partial', 'Unresolved']:
                            if cat in agents_df.columns:
                                resolution_data.append({
                                    'Agent': agent,
                                    'Resolution Type': cat,
                                    'Percentage': row[cat]
                                })
                    
                    resolution_df = pd.DataFrame(resolution_data)
                    
                    # Create stacked bar chart
                    fig = px.bar(
                        resolution_df,
                        x='Agent',
                        y='Percentage',
                        color='Resolution Type',
                        title="Resolution Status by Agent (%)",
                        labels={'Agent': 'Agent', 'Percentage': 'Percentage (%)'},
                        color_discrete_map={
                            'Resolved': 'green',
                            'Partial': 'gold',
                            'Unresolved': 'red'
                        }
                    )
                    fig.update_layout(yaxis_range=[0, 100])
                    st.plotly_chart(fig, use_container_width=True, key="agent_resolution_stacked_bar")
                
                # Sentiment Analysis by Agent
                if 'avg_sentiment_score' in agents_df.columns:
                    fig = px.bar(
                        agents_df.sort_values('avg_sentiment_score', ascending=False),
                        x='agent_name',
                        y='avg_sentiment_score',
                        title="Average Sentiment Score by Agent",
                        labels={'agent_name': 'Agent', 'avg_sentiment_score': 'Average Sentiment (-1 to 1)'},
                        color='avg_sentiment_score',
                        color_continuous_scale='RdYlGn'
                    )
                    fig.update_layout(yaxis_range=[-1, 1])
                    st.plotly_chart(fig, use_container_width=True, key="agent_sentiment_bar")
                
                # Individual Agent Analysis
                st.subheader("Individual Agent Analysis")
                
                # Get list of agents
                agents = sorted(filtered_data[agent_name_col].unique().tolist())
                selected_agent = st.selectbox("Select an agent:", agents)
                
                if selected_agent:
                    # Filter data for selected agent
                    agent_data = filtered_data[filtered_data[agent_name_col] == selected_agent]
                    
                    # Display key metrics for the selected agent
                    cols = st.columns(3)
                    
                    # Conversation count
                    with cols[0]:
                        st.metric("Conversations", len(agent_data))
                    
                    # Average rating
                    if service_rating_col and service_rating_col in agent_data.columns:
                        avg_rating = agent_data[service_rating_col].mean()
                        with cols[1]:
                            st.metric("Avg. Rating", f"{avg_rating:.2f}/10")
                    
                    # Resolution rate
                    if resolution_col and resolution_col in agent_data.columns:
                        # Convert to string and use case-insensitive comparison
                        resolved = agent_data[agent_data[resolution_col].astype(str).str.upper() == 'RESOLVED'].shape[0]
                        resolution_rate = resolved / len(agent_data) * 100 if len(agent_data) > 0 else 0
                        with cols[2]:
                            st.metric("Resolution Rate", f"{resolution_rate:.1f}%")
        else:
            st.warning("Agent performance analysis is not available: agent_name column missing or empty")
else:
    st.error("No data available. Please check your connection to Snowflake and verify that the TRANSCRIPT_ANALYSIS_RESULTS_FINAL table exists and contains data.")
    
    # Provide troubleshooting guidance
    st.subheader("Troubleshooting")
    st.markdown("""
    ### Possible issues:
    
    1. **Connection to Snowflake:** Verify that you can connect to Snowflake using the same credentials.
    
    2. **Table existence:** Confirm that the TRANSCRIPT_ANALYSIS_RESULTS_FINAL table exists in your Snowflake database.
    
    3. **Table permissions:** Ensure you have the necessary permissions to access the table.
    
    4. **Case sensitivity:** Snowflake identifiers are case-sensitive unless created with double quotes. The table might be stored with a different case.
    
    5. **Data availability:** Check if the table contains any data.
    
    Try using the simple_debug.py app to diagnose the specific issue.
    """) 