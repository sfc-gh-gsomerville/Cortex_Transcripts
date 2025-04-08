# Explicitly set required package versions for Snowflake compatibility
# requirements.txt
# snowflake-snowpark-python==1.5.0
# numpy==1.24.3 
# plotly==5.14.1
# pandas==1.5.3

import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime, timedelta

# Set page configuration - MUST be the first Streamlit command
st.set_page_config(
    page_title="Executive Dashboard",
    page_icon="📈",
    layout="wide"
)

# Initialize Snowflake session
session = st.connection('snowflake').session()

# Title and description
st.title("📈 Customer Support Executive Dashboard")
st.markdown("High-level insights from customer support transcripts for executive decision making")

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

# Function to get table columns for safe query building
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
            return columns_df['COLUMN_NAME'].tolist()
        else:
            return []
    except Exception as e:
        st.sidebar.error(f"Error getting columns: {e}")
        return []

# Function to load data with error handling
@st.cache_data(ttl=300)
def load_data():
    try:
        # Get available columns
        available_columns = get_table_columns()
        
        # Build a safe query with proper quoting
        if available_columns:
            # Use double quotes for all column names
            quoted_columns = [f'"{col}"' for col in available_columns]
            columns_str = ", ".join(quoted_columns)
            
            query = f"""
            SELECT {columns_str}
            FROM "TRANSCRIPT_ANALYSIS_RESULTS_FINAL"
            """
        else:
            # Fallback to SELECT * if we couldn't determine columns
            query = """
            SELECT * 
            FROM "TRANSCRIPT_ANALYSIS_RESULTS_FINAL"
            """
        
        # Show the query in debug
        with st.sidebar.expander("SQL Query", expanded=False):
            st.code(query)
            
        # Execute the query    
        df = session.sql(query).to_pandas()
        
        # Convert column names to lowercase for easier handling
        df.columns = [col.lower() for col in df.columns]
        
        # Debug: Show available columns
        with st.sidebar.expander("Available Columns", expanded=False):
            st.write(", ".join(df.columns.tolist()))
        
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
        st.sidebar.error(f"Error loading data: {e}")
        st.error(f"Failed to load data: {e}")
        # Return an empty DataFrame with expected columns
        expected_columns = ['conversation_id', 'start_time', 'service_rating', 
                           'sentiment_score', 'device_category', 'resolution']
        return pd.DataFrame(columns=expected_columns)

# Load data
df = load_data()

# Check if data is available
if df.empty:
    st.warning("No data is available. Please check your connection and table access.")
    st.stop()

# Date filter in sidebar
st.sidebar.header("Filters")

# Only create date filter if start_time column exists
if 'start_time' in df.columns and not df['start_time'].isna().all():
    min_date = df['start_time'].min().date()
    max_date = df['start_time'].max().date()
    
    # Default to last 30 days if date range is longer than that
    default_start = max_date - timedelta(days=30)
    default_start = max(default_start, min_date)
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df[(df['start_time'].dt.date >= start_date) & 
                        (df['start_time'].dt.date <= end_date)]
    else:
        df_filtered = df
else:
    st.sidebar.warning("Date filtering not available: start_time column missing or contains only null values")
    df_filtered = df

# Display filtered data count
st.sidebar.metric("Total Conversations", len(df_filtered))

# Create three columns for KPIs
col1, col2, col3 = st.columns(3)

# KPI 1: Average Customer Satisfaction
if 'service_rating' in df_filtered.columns and not df_filtered['service_rating'].isna().all():
    avg_satisfaction = df_filtered['service_rating'].mean()
    with col1:
        st.metric("Customer Satisfaction", f"{avg_satisfaction:.2f}/10")
else:
    with col1:
        st.metric("Customer Satisfaction", "N/A")
        st.caption("Service rating data not available")

# KPI 2: Resolution Rate
if 'resolution' in df_filtered.columns and not df_filtered['resolution'].isna().all():
    # Convert to string and use case-insensitive comparison
    resolved_count = df_filtered[df_filtered['resolution'].astype(str).str.upper() == 'RESOLVED'].shape[0]
    resolution_rate = (resolved_count / len(df_filtered)) * 100 if len(df_filtered) > 0 else 0
    with col2:
        st.metric("Resolution Rate", f"{resolution_rate:.1f}%")
else:
    with col2:
        st.metric("Resolution Rate", "N/A")
        st.caption("Resolution data not available")

# KPI 3: Average Sentiment Score
if 'sentiment_score' in df_filtered.columns and not df_filtered['sentiment_score'].isna().all():
    avg_sentiment = df_filtered['sentiment_score'].mean()
    sentiment_color = "normal" if avg_sentiment > 0 else "inverse"
    with col3:
        st.metric("Avg. Sentiment", f"{avg_sentiment:.2f}", delta_color=sentiment_color)
else:
    with col3:
        st.metric("Avg. Sentiment", "N/A")
        st.caption("Sentiment data not available")

# Create tabs for different sections
tab1, tab2, tab3 = st.tabs(["Trends", "Categories", "Performance"])

# Tab 1: Trends
with tab1:
    st.header("KPI Trends")
    
    if 'start_time' in df_filtered.columns and not df_filtered['start_time'].isna().all():
        # Group by week for trend analysis
        df_filtered['week'] = df_filtered['start_time'].dt.to_period('W').astype(str)
        
        # Prepare for multiple metrics
        metrics_to_track = []
        weekly_data = df_filtered.groupby('week').size().reset_index()
        weekly_data.columns = ['Week', 'Conversation Count']
        metrics_to_track.append('Conversation Count')
        
        # Add service rating if available
        if 'service_rating' in df_filtered.columns and not df_filtered['service_rating'].isna().all():
            weekly_rating = df_filtered.groupby('week')['service_rating'].mean().reset_index()
            weekly_rating.columns = ['Week', 'Avg. Satisfaction']
            weekly_data = weekly_data.merge(weekly_rating, on='Week')
            metrics_to_track.append('Avg. Satisfaction')
        
        # Add sentiment if available
        if 'sentiment_score' in df_filtered.columns and not df_filtered['sentiment_score'].isna().all():
            weekly_sentiment = df_filtered.groupby('week')['sentiment_score'].mean().reset_index()
            weekly_sentiment.columns = ['Week', 'Avg. Sentiment']
            weekly_data = weekly_data.merge(weekly_sentiment, on='Week')
            metrics_to_track.append('Avg. Sentiment')
        
        # Add resolution rate if available
        if 'resolution' in df_filtered.columns and not df_filtered['resolution'].isna().all():
            def calc_weekly_resolution_rate(group):
                # Convert to string and use case-insensitive comparison
                resolved = group[group['resolution'].astype(str).str.upper() == 'RESOLVED'].shape[0]
                return (resolved / len(group)) * 100 if len(group) > 0 else 0
            
            weekly_resolution = df_filtered.groupby('week').apply(calc_weekly_resolution_rate).reset_index()
            weekly_resolution.columns = ['Week', 'Resolution Rate (%)']
            weekly_data = weekly_data.merge(weekly_resolution, on='Week')
            metrics_to_track.append('Resolution Rate (%)')
        
        # Plot trends
        if metrics_to_track:
            st.subheader("Weekly Trends")
            
            # Let the user select metrics to display
            selected_metrics = st.multiselect(
                "Select metrics to display",
                options=metrics_to_track,
                default=metrics_to_track[:min(3, len(metrics_to_track))]
            )
            
            if selected_metrics:
                fig = px.line(
                    weekly_data, 
                    x='Week', 
                    y=selected_metrics,
                    title="Weekly KPI Trends",
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Please select at least one metric to display")
    else:
        st.warning("Cannot show trends: date information is not available")

# Tab 2: Categories
with tab2:
    st.header("Conversation Categories")
    
    # Row 1: Device Categories
    if 'device_category' in df_filtered.columns and not df_filtered['device_category'].isna().all():
        st.subheader("Device Categories")
        
        # Get device category distribution
        device_counts = df_filtered['device_category'].value_counts().reset_index()
        device_counts.columns = ['Device Category', 'Count']
        
        # Calculate percentage
        device_counts['Percentage'] = (device_counts['Count'] / device_counts['Count'].sum() * 100).round(1)
        device_counts['Label'] = device_counts['Device Category'] + ' (' + device_counts['Percentage'].astype(str) + '%)'
        
        # Create pie chart
        fig = px.pie(
            device_counts, 
            values='Count', 
            names='Label',
            title="Conversations by Device Category"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Show KPIs by device category if available
        cols = st.columns(2)
        
        # Left column: Satisfaction by device
        if 'service_rating' in df_filtered.columns and not df_filtered['service_rating'].isna().all():
            with cols[0]:
                rating_by_device = df_filtered.groupby('device_category')['service_rating'].mean().reset_index()
                rating_by_device.columns = ['Device Category', 'Avg. Satisfaction']
                
                fig = px.bar(
                    rating_by_device.sort_values('Avg. Satisfaction', ascending=False),
                    x='Device Category',
                    y='Avg. Satisfaction',
                    title="Satisfaction by Device Category",
                    color='Avg. Satisfaction',
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(yaxis_range=[0, 10])
                st.plotly_chart(fig, use_container_width=True)
        
        # Right column: Resolution rate by device
        if 'resolution' in df_filtered.columns and not df_filtered['resolution'].isna().all():
            with cols[1]:
                # Calculate resolution rate by device with case-insensitive comparison
                device_resolution = df_filtered.groupby('device_category').apply(
                    lambda x: (x['resolution'].astype(str).str.upper() == 'RESOLVED').mean() * 100
                ).reset_index()
                device_resolution.columns = ['Device Category', 'Resolution Rate (%)']
                
                fig = px.bar(
                    device_resolution.sort_values('Resolution Rate (%)', ascending=False),
                    x='Device Category',
                    y='Resolution Rate (%)',
                    title="Resolution Rate by Device Category",
                    color='Resolution Rate (%)',
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(yaxis_range=[0, 100])
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Device category information is not available")
    
    # Row 2: Main Issues (if available)
    if 'main_issue_answer' in df_filtered.columns and not df_filtered['main_issue_answer'].isna().all():
        st.subheader("Top Customer Issues")
        
        # Get main issues distribution
        issue_counts = df_filtered['main_issue_answer'].value_counts().reset_index()
        issue_counts.columns = ['Issue', 'Count']
        
        # Show top issues
        top_n = min(10, len(issue_counts))
        top_issues = issue_counts.head(top_n)
        
        fig = px.bar(
            top_issues,
            x='Count',
            y='Issue',
            title=f"Top {top_n} Customer Issues",
            orientation='h',
            color='Count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)
    
# Tab 3: Performance
with tab3:
    st.header("Performance Insights")
    
    # Resolution Analysis
    if 'resolution' in df_filtered.columns and not df_filtered['resolution'].isna().all():
        st.subheader("Resolution Analysis")
        
        # Get resolution distribution
        resolution_counts = df_filtered['resolution'].value_counts().reset_index()
        resolution_counts.columns = ['Resolution', 'Count']
        
        # Calculate percentage
        resolution_counts['Percentage'] = (resolution_counts['Count'] / resolution_counts['Count'].sum() * 100).round(1)
        resolution_counts['Label'] = resolution_counts['Resolution'] + ' (' + resolution_counts['Percentage'].astype(str) + '%)'
        
        # Create pie chart
        fig = px.pie(
            resolution_counts, 
            values='Count', 
            names='Label',
            title="Conversation Resolutions",
            color='Resolution',
            color_discrete_map={
                'Resolved': 'green',
                'Partial': 'orange',
                'Unresolved': 'red'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Resolution data is not available")
    
    # Sentiment Distribution
    if 'sentiment_score' in df_filtered.columns and not df_filtered['sentiment_score'].isna().all():
        st.subheader("Sentiment Distribution")
        
        # Create sentiment categories
        df_filtered['sentiment_category'] = pd.cut(
            df_filtered['sentiment_score'],
            bins=[-1, -0.33, 0.33, 1],
            labels=['Negative', 'Neutral', 'Positive']
        )
        
        # Get sentiment distribution
        sentiment_counts = df_filtered['sentiment_category'].value_counts().reset_index()
        sentiment_counts.columns = ['Sentiment', 'Count']
        
        # Calculate percentage
        sentiment_counts['Percentage'] = (sentiment_counts['Count'] / sentiment_counts['Count'].sum() * 100).round(1)
        sentiment_counts['Label'] = sentiment_counts['Sentiment'] + ' (' + sentiment_counts['Percentage'].astype(str) + '%)'
        
        # Create pie chart
        fig = px.pie(
            sentiment_counts, 
            values='Count', 
            names='Label',
            title="Sentiment Distribution",
            color='Sentiment',
            color_discrete_map={
                'Positive': 'green',
                'Neutral': 'gray',
                'Negative': 'red'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Sentiment data is not available")
    
    # Customer Satisfaction Distribution
    if 'service_rating' in df_filtered.columns and not df_filtered['service_rating'].isna().all():
        st.subheader("Customer Satisfaction Distribution")
        
        # Create histogram
        fig = px.histogram(
            df_filtered,
            x='service_rating',
            nbins=10,
            color_discrete_sequence=['blue'],
            title="Distribution of Customer Satisfaction Ratings"
        )
        fig.update_layout(xaxis_title="Rating (0-10)", yaxis_title="Number of Conversations")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Customer satisfaction data is not available")
        
    # Agent Performance Analysis
    if 'agent_name' in df_filtered.columns and not df_filtered['agent_name'].isna().all():
        st.subheader("Agent Performance Analysis")
        
        # Calculate metrics by agent
        agents_df = pd.DataFrame()
        
        # Count conversations by agent
        conversation_counts = df_filtered['agent_name'].value_counts().reset_index()
        conversation_counts.columns = ['agent_name', 'conversation_count']
        agents_df = conversation_counts
        
        # Calculate average service rating by agent if available
        if 'service_rating' in df_filtered.columns and not df_filtered['service_rating'].isna().all():
            avg_ratings = df_filtered.groupby('agent_name')['service_rating'].mean().reset_index()
            avg_ratings.columns = ['agent_name', 'avg_service_rating']
            agents_df = agents_df.merge(avg_ratings, on='agent_name', how='left')
        
        # Calculate average sentiment score by agent if available
        if 'sentiment_score' in df_filtered.columns and not df_filtered['sentiment_score'].isna().all():
            avg_sentiment = df_filtered.groupby('agent_name')['sentiment_score'].mean().reset_index()
            avg_sentiment.columns = ['agent_name', 'avg_sentiment_score']
            agents_df = agents_df.merge(avg_sentiment, on='agent_name', how='left')
        
        # Calculate resolution rate by agent if available
        if 'resolution' in df_filtered.columns and not df_filtered['resolution'].isna().all():
            def calc_resolution_rate(group):
                # Convert to string and use case-insensitive comparison
                resolved = group[group['resolution'].astype(str).str.upper() == 'RESOLVED'].shape[0]
                return (resolved / len(group)) * 100 if len(group) > 0 else 0
            
            resolution_rates = df_filtered.groupby('agent_name').apply(calc_resolution_rate).reset_index()
            resolution_rates.columns = ['agent_name', 'resolution_rate']
            agents_df = agents_df.merge(resolution_rates, on='agent_name', how='left')
        
        # Calculate executive-focused performance score
        # For executive dashboard, prioritize resolution rate and service rating
        if 'avg_service_rating' in agents_df.columns and 'resolution_rate' in agents_df.columns:
            # Normalize metrics to 0-1 scale for fair comparison
            agents_df['norm_rating'] = agents_df['avg_service_rating'] / 10  # Assuming 10 is max rating
            agents_df['norm_resolution'] = agents_df['resolution_rate'] / 100
            
            if 'avg_sentiment_score' in agents_df.columns:
                # Transform sentiment from [-1,1] to [0,1]
                agents_df['norm_sentiment'] = (agents_df['avg_sentiment_score'] + 1) / 2
                # Executive performance score (prioritizing CSAT and resolution)
                agents_df['exec_score'] = (
                    agents_df['norm_rating'] * 0.5 + 
                    agents_df['norm_resolution'] * 0.4 + 
                    agents_df['norm_sentiment'] * 0.1
                ) * 100
            else:
                # Score without sentiment
                agents_df['exec_score'] = (
                    agents_df['norm_rating'] * 0.6 + 
                    agents_df['norm_resolution'] * 0.4
                ) * 100
        
        # Show comparison visualizations
        columns = st.columns(2)
        
        # Top Agents by Resolution Rate
        with columns[0]:
            if 'resolution_rate' in agents_df.columns:
                # Get top 5 agents by resolution rate
                top_agents = agents_df.sort_values('resolution_rate', ascending=False).head(5)
                
                fig = px.bar(
                    top_agents,
                    x='agent_name',
                    y='resolution_rate',
                    title="Top Agents by Resolution Rate",
                    labels={'agent_name': 'Agent', 'resolution_rate': 'Resolution Rate (%)'},
                    color='resolution_rate',
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(xaxis_title="Agent", yaxis_range=[0, 100])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Resolution rate data not available")
        
        # Top Agents by Customer Satisfaction
        with columns[1]:
            if 'avg_service_rating' in agents_df.columns:
                # Get top 5 agents by service rating
                top_agents = agents_df.sort_values('avg_service_rating', ascending=False).head(5)
                
                fig = px.bar(
                    top_agents,
                    x='agent_name',
                    y='avg_service_rating',
                    title="Top Agents by Customer Satisfaction",
                    labels={'agent_name': 'Agent', 'avg_service_rating': 'Avg. Satisfaction (0-10)'},
                    color='avg_service_rating',
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(xaxis_title="Agent", yaxis_range=[0, 10])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Service rating data not available")
        
        # Performance Score Visualization (all agents)
        if 'exec_score' in agents_df.columns:
            # Show all agents ranked by performance score
            fig = px.bar(
                agents_df.sort_values('exec_score', ascending=False),
                x='agent_name',
                y='exec_score',
                title="Agent Performance Score",
                labels={'agent_name': 'Agent', 'exec_score': 'Performance Score (%)'},
                color='exec_score',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(xaxis_title="Agent", yaxis_range=[0, 100])
            st.plotly_chart(fig, use_container_width=True)
            
            # Explain the performance score
            with st.expander("About the Executive Performance Score"):
                st.write("""
                The Executive Performance Score is a weighted metric combining:
                
                - 50% Customer Satisfaction Rating
                - 40% Resolution Rate
                - 10% Customer Sentiment
                
                This weighting emphasizes customer satisfaction as the primary success metric.
                """)
        
        # Format and display the performance table
        st.subheader("Agent Performance Details")
        
        # Format table for display
        display_df = agents_df.copy()
        
        if 'avg_service_rating' in display_df.columns:
            display_df['avg_service_rating'] = display_df['avg_service_rating'].round(2)
        if 'avg_sentiment_score' in display_df.columns:
            display_df['avg_sentiment_score'] = display_df['avg_sentiment_score'].round(2)
        if 'resolution_rate' in display_df.columns:
            display_df['resolution_rate'] = display_df['resolution_rate'].round(1).astype(str) + '%'
        if 'exec_score' in display_df.columns:
            display_df['exec_score'] = display_df['exec_score'].round(1).astype(str) + '%'
            
        # Rename columns for better display
        column_rename = {
            'agent_name': 'Agent',
            'conversation_count': 'Conversations',
            'avg_service_rating': 'Satisfaction Rating',
            'avg_sentiment_score': 'Sentiment Score',
            'resolution_rate': 'Resolution Rate',
            'exec_score': 'Performance Score'
        }
        display_df = display_df.rename(columns=column_rename)
        
        # Remove calculation columns
        display_columns = [col for col in display_df.columns if not col.startswith('norm_')]
        
        # Display table
        st.dataframe(display_df[display_columns], use_container_width=True)
        
        # Add download button for the agent performance data
        csv = display_df[display_columns].to_csv(index=False)
        st.download_button(
            label="Download Agent Performance Data",
            data=csv,
            file_name="agent_performance.csv",
            mime="text/csv"
        )
    else:
        st.warning("Agent performance analysis not available: agent_name column is missing or empty") 