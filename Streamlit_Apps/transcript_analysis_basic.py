# Explicitly set required package versions for Snowflake compatibility
# requirements.txt
# snowflake-snowpark-python==1.5.0
# numpy==1.24.3 
# plotly==5.14.1
# pandas==1.5.3

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# Set page configuration - MUST be the first Streamlit command
st.set_page_config(
    page_title="Transcript Analysis",
    page_icon="📋",
    layout="wide",
)

# Title and description
st.title("📋 Customer Support Transcript Analysis")
st.markdown("Basic analysis of customer support transcripts")

# Initialize Snowflake session
session = st.connection('snowflake').session()

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

# Function to get table columns
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

# Function to load data
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
            ORDER BY "START_TIME" DESC
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
        # Return an empty DataFrame with expected columns
        expected_columns = ['conversation_id', 'source', 'start_time', 'service_rating', 
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
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
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

# Category filter if device_category exists
if 'device_category' in df.columns and not df['device_category'].isna().all():
    categories = ['All'] + sorted(df_filtered['device_category'].unique().tolist())
    selected_category = st.sidebar.selectbox("Device Category", categories)
    
    if selected_category != 'All':
        df_filtered = df_filtered[df_filtered['device_category'] == selected_category]
else:
    st.sidebar.warning("Device category filtering not available")

# Display count of filtered records
st.write(f"Analyzing {len(df_filtered)} customer support transcripts")

# Create tabs for different analyses
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Sentiment Analysis", "Transcript Viewer", "Agent Performance"])

# Tab 1: Overview
with tab1:
    st.header("Overview")
    
    # Create three columns for metrics
    col1, col2, col3 = st.columns(3)
    
    # Service rating metric
    if 'service_rating' in df_filtered.columns and not df_filtered['service_rating'].isna().all():
        avg_rating = df_filtered['service_rating'].mean()
        with col1:
            st.metric("Average Service Rating", f"{avg_rating:.2f}/10")
    else:
        with col1:
            st.warning("Service rating data not available")
    
    # Sentiment score metric
    if 'sentiment_score' in df_filtered.columns and not df_filtered['sentiment_score'].isna().all():
        avg_sentiment = df_filtered['sentiment_score'].mean()
        sentiment_color = "normal" if avg_sentiment > 0 else "inverse"
        with col2:
            st.metric("Average Sentiment Score", f"{avg_sentiment:.2f}", delta_color=sentiment_color)
    else:
        with col2:
            st.warning("Sentiment score data not available")
    
    # Resolution rate metric
    if 'resolution' in df_filtered.columns and not df_filtered['resolution'].isna().all():
        # Convert to string and use case-insensitive comparison
        resolved = df_filtered[df_filtered['resolution'].astype(str).str.upper() == 'RESOLVED'].shape[0]
        resolution_rate = resolved / len(df_filtered) * 100 if len(df_filtered) > 0 else 0
        with col3:
            st.metric("Resolution Rate", f"{resolution_rate:.1f}%")
    else:
        with col3:
            st.warning("Resolution data not available")
    
    # Device Category Distribution
    if 'device_category' in df_filtered.columns and not df_filtered['device_category'].isna().all():
        st.subheader("Device Category Distribution")
        
        device_counts = df_filtered['device_category'].value_counts().reset_index()
        device_counts.columns = ['Device Category', 'Count']
        
        fig = px.bar(
            device_counts.sort_values('Count', ascending=False),
            x='Device Category',
            y='Count',
            title="Distribution by Device Category",
            color='Count',
            color_continuous_scale='blues'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Device category data not available")
    
    # Conversations over time
    if 'start_time' in df_filtered.columns and not df_filtered['start_time'].isna().all():
        st.subheader("Conversations Over Time")
        
        # Group by date
        df_filtered['date'] = df_filtered['start_time'].dt.date
        daily_counts = df_filtered.groupby('date').size().reset_index()
        daily_counts.columns = ['Date', 'Count']
        
        fig = px.line(
            daily_counts, 
            x='Date', 
            y='Count',
            title="Daily Conversation Volume",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Date data not available for time analysis")

# Tab 2: Sentiment Analysis
with tab2:
    st.header("Sentiment Analysis")
    
    if 'sentiment_score' in df_filtered.columns and not df_filtered['sentiment_score'].isna().all():
        # Sentiment distribution
        st.subheader("Sentiment Score Distribution")
        
        fig = px.histogram(
            df_filtered,
            x='sentiment_score',
            nbins=20,
            title="Distribution of Sentiment Scores",
            color_discrete_sequence=['blue']
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Create sentiment categories for analysis
        df_filtered['sentiment_category'] = pd.cut(
            df_filtered['sentiment_score'],
            bins=[-1, -0.33, 0.33, 1],
            labels=['Negative', 'Neutral', 'Positive']
        )
        
        # Sentiment by category
        sentiment_counts = df_filtered['sentiment_category'].value_counts().reset_index()
        sentiment_counts.columns = ['Category', 'Count']
        
        fig = px.pie(
            sentiment_counts,
            values='Count',
            names='Category',
            title="Sentiment Categories",
            color='Category',
            color_discrete_map={
                'Positive': 'green',
                'Neutral': 'gray',
                'Negative': 'red'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Device category and sentiment
        if 'device_category' in df_filtered.columns and not df_filtered['device_category'].isna().all():
            st.subheader("Sentiment by Device Category")
            
            sentiment_by_device = df_filtered.groupby('device_category')['sentiment_score'].mean().reset_index()
            sentiment_by_device.columns = ['Device Category', 'Average Sentiment']
            
            fig = px.bar(
                sentiment_by_device.sort_values('Average Sentiment'),
                x='Device Category',
                y='Average Sentiment',
                title="Average Sentiment by Device Category",
                color='Average Sentiment',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Sentiment score data not available")

# Tab 3: Transcript Viewer
with tab3:
    st.header("Transcript Viewer")
    
    if 'transcript' in df_filtered.columns:
        # Search functionality
        search_term = st.text_input("Search in transcripts", "")
        
        if search_term:
            matching_df = df_filtered[df_filtered['transcript'].str.contains(search_term, case=False, na=False)]
            st.write(f"Found {len(matching_df)} matching transcripts")
            
            if not matching_df.empty:
                for i, row in matching_df.head(10).iterrows():
                    # Create a clean title for the expander
                    if 'start_time' in row and not pd.isna(row['start_time']):
                        title = f"Transcript {i} - {row['start_time'].strftime('%Y-%m-%d %H:%M')}"
                    else:
                        title = f"Transcript {i} - No date"
                        
                    with st.expander(title):
                        # Show metadata if available
                        cols = st.columns(3)
                        
                        if 'device_category' in row and not pd.isna(row['device_category']):
                            cols[0].info(f"Device: {row['device_category']}")
                            
                        if 'sentiment_score' in row and not pd.isna(row['sentiment_score']):
                            cols[1].info(f"Sentiment: {row['sentiment_score']:.2f}")
                            
                        if 'resolution' in row and not pd.isna(row['resolution']):
                            cols[2].info(f"Resolution: {row['resolution']}")
                        
                        # Show transcript
                        st.text_area("Transcript", row['transcript'], height=200)
        else:
            # Just show the most recent transcripts
            for i, row in df_filtered.head(5).iterrows():
                # Create a clean title for the expander
                if 'start_time' in row and not pd.isna(row['start_time']):
                    title = f"Transcript {i} - {row['start_time'].strftime('%Y-%m-%d %H:%M')}"
                else:
                    title = f"Transcript {i} - No date"
                    
                with st.expander(title):
                    # Show metadata if available
                    cols = st.columns(3)
                    
                    if 'device_category' in row and not pd.isna(row['device_category']):
                        cols[0].info(f"Device: {row['device_category']}")
                        
                    if 'sentiment_score' in row and not pd.isna(row['sentiment_score']):
                        cols[1].info(f"Sentiment: {row['sentiment_score']:.2f}")
                        
                    if 'resolution' in row and not pd.isna(row['resolution']):
                        cols[2].info(f"Resolution: {row['resolution']}")
                    
                    # Show transcript
                    st.text_area("Transcript", row['transcript'], height=200)
    else:
        st.warning("Transcript data not available")

# Tab 4: Agent Performance
with tab4:
    st.header("Agent Performance")
    
    if 'agent_name' in df_filtered.columns and not df_filtered['agent_name'].isna().all():
        # Calculate key metrics by agent
        st.subheader("Agent Performance Metrics")
        
        # Count conversations by agent
        conversation_counts = df_filtered['agent_name'].value_counts().reset_index()
        conversation_counts.columns = ['Agent', 'Conversations']
        
        # Prepare metrics dataframe
        agent_metrics = conversation_counts.copy()
        
        # Add service rating if available
        if 'service_rating' in df_filtered.columns and not df_filtered['service_rating'].isna().all():
            avg_ratings = df_filtered.groupby('agent_name')['service_rating'].mean().reset_index()
            avg_ratings.columns = ['Agent', 'Avg. Rating']
            avg_ratings['Avg. Rating'] = avg_ratings['Avg. Rating'].round(2)
            agent_metrics = agent_metrics.merge(avg_ratings, on='Agent', how='left')
        
        # Add sentiment score if available
        if 'sentiment_score' in df_filtered.columns and not df_filtered['sentiment_score'].isna().all():
            avg_sentiment = df_filtered.groupby('agent_name')['sentiment_score'].mean().reset_index()
            avg_sentiment.columns = ['Agent', 'Avg. Sentiment']
            avg_sentiment['Avg. Sentiment'] = avg_sentiment['Avg. Sentiment'].round(2)
            agent_metrics = agent_metrics.merge(avg_sentiment, on='Agent', how='left')
        
        # Add resolution rate if available
        if 'resolution' in df_filtered.columns and not df_filtered['resolution'].isna().all():
            def calc_resolution_rate(group):
                # Convert to string and use case-insensitive comparison
                resolved = group[group['resolution'].astype(str).str.upper() == 'RESOLVED'].shape[0]
                return (resolved / len(group)) * 100 if len(group) > 0 else 0
            
            resolution_rates = df_filtered.groupby('agent_name').apply(calc_resolution_rate).reset_index()
            resolution_rates.columns = ['Agent', 'Resolution Rate (%)']
            resolution_rates['Resolution Rate (%)'] = resolution_rates['Resolution Rate (%)'].round(1)
            agent_metrics = agent_metrics.merge(resolution_rates, on='Agent', how='left')
        
        # Display agent metrics table
        st.dataframe(agent_metrics, use_container_width=True)
        
        # Create visualizations for agent comparisons
        st.subheader("Agent Comparisons")
        
        metric_options = ["Conversations"]
        if 'Avg. Rating' in agent_metrics.columns:
            metric_options.append("Avg. Rating")
        if 'Avg. Sentiment' in agent_metrics.columns:
            metric_options.append("Avg. Sentiment")
        if 'Resolution Rate (%)' in agent_metrics.columns:
            metric_options.append("Resolution Rate (%)")
            
        # Let user select which metric to visualize
        selected_metric = st.selectbox("Select metric to compare:", metric_options)
        
        # Create visualization based on selected metric
        if selected_metric:
            fig = px.bar(
                agent_metrics.sort_values(selected_metric, ascending=False),
                x='Agent',
                y=selected_metric,
                title=f"{selected_metric} by Agent",
                color=selected_metric,
                color_continuous_scale='Blues' if selected_metric == "Conversations" else 'RdYlGn'
            )
            
            # Set appropriate y-axis range for different metrics
            if selected_metric == "Avg. Rating":
                fig.update_layout(yaxis_range=[0, 10])
            elif selected_metric == "Avg. Sentiment":
                fig.update_layout(yaxis_range=[-1, 1])
            elif selected_metric == "Resolution Rate (%)":
                fig.update_layout(yaxis_range=[0, 100])
                
            st.plotly_chart(fig, use_container_width=True)
        
        # Individual agent analysis
        st.subheader("Individual Agent Analysis")
        
        # Get list of agents
        agents = sorted(df_filtered['agent_name'].unique().tolist())
        selected_agent = st.selectbox("Select an agent:", agents)
        
        if selected_agent:
            # Filter data for selected agent
            agent_data = df_filtered[df_filtered['agent_name'] == selected_agent]
            
            # Display key metrics for the selected agent
            cols = st.columns(3)
            
            # Conversation count
            with cols[0]:
                st.metric("Conversations", len(agent_data))
            
            # Average rating
            if 'service_rating' in agent_data.columns and not agent_data['service_rating'].isna().all():
                avg_rating = agent_data['service_rating'].mean()
                with cols[1]:
                    st.metric("Avg. Rating", f"{avg_rating:.2f}/10")
            
            # Resolution rate
            if 'resolution' in agent_data.columns and not agent_data['resolution'].isna().all():
                # Convert to string and use case-insensitive comparison
                resolved = agent_data[agent_data['resolution'].astype(str).str.upper() == 'RESOLVED'].shape[0]
                resolution_rate = resolved / len(agent_data) * 100 if len(agent_data) > 0 else 0
                with cols[2]:
                    st.metric("Resolution Rate", f"{resolution_rate:.1f}%")
            
            # Show device category breakdown if available
            if 'device_category' in agent_data.columns and not agent_data['device_category'].isna().all():
                st.subheader(f"Device Categories Handled by {selected_agent}")
                
                device_counts = agent_data['device_category'].value_counts().reset_index()
                device_counts.columns = ['Device Category', 'Count']
                
                fig = px.pie(
                    device_counts, 
                    values='Count', 
                    names='Device Category',
                    title=f"Device Categories Handled by {selected_agent}"
                )
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Agent performance analysis is not available: agent_name column missing or empty") 