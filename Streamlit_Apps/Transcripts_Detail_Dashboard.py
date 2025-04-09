import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
from statistics import mean, median, mode

# Set page config - must be the first Streamlit command
st.set_page_config(
    page_title="Transcript Detail Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Page title and description
st.title("Transcript Detail Dashboard")
st.markdown("Detailed analysis of customer support transcripts with comprehensive filtering and metrics")

# Initialize Snowflake session
try:
    session = get_active_session()
    
    # Display session info for debugging
    try:
        session_info = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()").collect()
        st.sidebar.success(f"Connected to Snowflake: {session_info[0][0]}.{session_info[0][1]} using {session_info[0][2]}")
    except:
        st.sidebar.success("Connected to Snowflake")
        
except Exception as e:
    st.sidebar.error(f"Failed to connect to Snowflake: {e}")
    st.stop()

# Function to load data with error handling
@st.cache_data(ttl=600)
def load_data():
    try:
        # Try different database specifications in case the fully qualified name is needed
        queries = [
            # Option 1: Unqualified table name (relies on current session context)
            """
            SELECT * 
            FROM TRANSCRIPT_ANALYSIS_RESULTS_FINAL 
            ORDER BY START_TIME DESC
            LIMIT 1000
            """,
            
            # Option 2: With database and schema qualification - adjust if needed
            """
            SELECT * 
            FROM MED_DEVICE_TRANSCRIPTS.PUBLIC.TRANSCRIPT_ANALYSIS_RESULTS_FINAL 
            ORDER BY START_TIME DESC
            LIMIT 1000
            """,
            
            # Option 3: Using quoted identifiers
            """
            SELECT *
            FROM "TRANSCRIPT_ANALYSIS_RESULTS_FINAL"
            ORDER BY "START_TIME" DESC
            LIMIT 1000
            """
        ]
        
        # Try each query until one works
        df = pd.DataFrame()
        last_error = None
        
        for i, query in enumerate(queries):
            try:
                st.sidebar.expander(f"SQL Query Option {i+1}").write(query)
                df = session.sql(query).to_pandas()
                if not df.empty:
                    st.sidebar.success(f"Query option {i+1} succeeded!")
                    break
            except Exception as e:
                last_error = str(e)
                continue
        
        if df.empty and last_error:
            st.warning(f"All queries failed. Last error: {last_error}")
            
            # Try to list tables to help debugging
            try:
                tables = session.sql("SHOW TABLES").collect()
                st.sidebar.expander("Available Tables").write(pd.DataFrame(tables))
            except:
                st.sidebar.warning("Could not list available tables")
                
            return pd.DataFrame()
        
        # Debug information
        if df.empty:
            st.warning("No data was returned from the query.")
            return pd.DataFrame()
        
        # Ensure column names are lowercase for consistent access
        df.columns = [col.lower() for col in df.columns]
        
        # Convert date columns to datetime
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'])
        if 'end_time' in df.columns:
            df['end_time'] = pd.to_datetime(df['end_time'])
        
        # Calculate duration in minutes
        if 'start_time' in df.columns and 'end_time' in df.columns:
            df['duration_minutes'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60
        
        # Convert service_rating to numeric
        if 'service_rating' in df.columns:
            df['service_rating_numeric'] = pd.to_numeric(df['service_rating'], errors='coerce')
        
        return df
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        import traceback
        st.code(traceback.format_exc())
        # Return empty DataFrame with expected columns
        return pd.DataFrame()

# Load the data
df = load_data()

# Add a debug expander to show available columns and data sample
with st.sidebar.expander("Debug Info"):
    st.write("Available columns:", df.columns.tolist())
    st.write("Data shape:", df.shape)
    if not df.empty:
        st.write("First row:", df.iloc[0])
    else:
        st.write("DataFrame is empty")

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
if 'start_time' in df.columns and not df.empty:
    min_date = df['start_time'].min().date()
    max_date = df['start_time'].max().date()
    
    default_start = max_date - timedelta(days=30)
    if default_start < min_date:
        default_start = min_date
    
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
    df_filtered = df
    
# Agent filter
if 'agent_name' in df.columns and not df.empty:
    agents = ['All'] + sorted(df['agent_name'].unique().tolist())
    selected_agent = st.sidebar.selectbox("Agent", agents)
    
    if selected_agent != 'All':
        df_filtered = df_filtered[df_filtered['agent_name'] == selected_agent]

# Sentiment category filter
if 'sentiment_category' in df.columns and not df.empty:
    sentiment_categories = ['All'] + sorted(df['sentiment_category'].unique().tolist())
    selected_sentiment = st.sidebar.selectbox("Sentiment Category", sentiment_categories)
    
    if selected_sentiment != 'All':
        df_filtered = df_filtered[df_filtered['sentiment_category'] == selected_sentiment]

# Device category filter
if 'device_category' in df.columns and not df.empty:
    device_categories = ['All'] + sorted(df['device_category'].unique().tolist())
    selected_device = st.sidebar.selectbox("Device Category", device_categories)
    
    if selected_device != 'All':
        df_filtered = df_filtered[df_filtered['device_category'] == selected_device]

# Resolution filter
if 'resolution' in df.columns and not df.empty:
    resolutions = ['All'] + sorted(df['resolution'].unique().tolist())
    selected_resolution = st.sidebar.selectbox("Resolution", resolutions)
    
    if selected_resolution != 'All':
        df_filtered = df_filtered[df_filtered['resolution'] == selected_resolution]

# Service rating filter
if 'service_rating_numeric' in df.columns and not df.empty:
    min_rating = int(df['service_rating_numeric'].min())
    max_rating = int(df['service_rating_numeric'].max())
    
    selected_rating_range = st.sidebar.slider(
        "Service Rating Range", 
        min_value=min_rating,
        max_value=max_rating,
        value=(min_rating, max_rating)
    )
    
    df_filtered = df_filtered[
        (df_filtered['service_rating_numeric'] >= selected_rating_range[0]) & 
        (df_filtered['service_rating_numeric'] <= selected_rating_range[1])
    ]

# Calculate the service index for each record
def calculate_service_index(row):
    try:
        # Resolution component (60%)
        resolution_score = 0
        if row.get('resolution') == 'Resolved':
            resolution_score = 10
        elif row.get('resolution') == 'Partial':
            resolution_score = 5
        
        # Service rating component (40%)
        service_rating = row.get('service_rating_numeric', 0) 
        if pd.isna(service_rating):
            service_rating = 0
            
        # Combine scores with weights
        service_index = (0.6 * resolution_score) + (0.4 * service_rating)
        return round(service_index, 1)
    except:
        return 0

# Apply service index calculation
if not df_filtered.empty and 'resolution' in df_filtered.columns:
    df_filtered['service_index'] = df_filtered.apply(calculate_service_index, axis=1)

# Main dashboard content
if df_filtered.empty:
    st.warning("No data available with the current filters. Please adjust your filters.")
else:
    # Key metrics section
    st.header("Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_transcripts = len(df_filtered)
        st.metric("Total Transcripts", f"{total_transcripts:,}")
    
    with col2:
        if 'sentiment_score' in df_filtered.columns:
            avg_sentiment = df_filtered['sentiment_score'].mean()
            st.metric("Avg Sentiment Score", f"{avg_sentiment:.2f}")
        else:
            st.metric("Avg Sentiment Score", "N/A")
    
    with col3:
        if 'service_rating_numeric' in df_filtered.columns:
            avg_rating = df_filtered['service_rating_numeric'].mean()
            st.metric("Avg Service Rating", f"{avg_rating:.2f}/10")
        else:
            st.metric("Avg Service Rating", "N/A")
    
    with col4:
        if 'service_index' in df_filtered.columns:
            avg_service_index = df_filtered['service_index'].mean()
            st.metric("Avg Service Index", f"{avg_service_index:.2f}/10")
        else:
            st.metric("Avg Service Index", "N/A")
    
    # Calls per day visualization
    st.subheader("Calls per Day")
    if 'start_time' in df_filtered.columns:
        calls_per_day = df_filtered.resample('D', on='start_time').size().reset_index()
        calls_per_day.columns = ['Date', 'Count']
        
        fig = px.line(
            calls_per_day, 
            x='Date', 
            y='Count',
            markers=True,
            labels={'Count': 'Number of Calls', 'Date': 'Date'},
            height=300
        )
        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Date information is not available to show calls per day.")
    
    # Create three columns for device, sentiment, and resolution distributions
    col1, col2, col3 = st.columns(3)
    
    # Device category distribution
    with col1:
        st.subheader("Device Categories")
        if 'device_category' in df_filtered.columns:
            device_counts = df_filtered['device_category'].value_counts()
            device_percentages = df_filtered['device_category'].value_counts(normalize=True) * 100
            
            device_data = pd.DataFrame({
                'Device': device_counts.index,
                'Count': device_counts.values,
                'Percentage': device_percentages.values
            })
            
            fig = px.pie(
                device_data,
                values='Count',
                names='Device',
                hole=0.4,
                labels={'Device': 'Device Category'},
                hover_data=['Percentage'],
                custom_data=['Count', 'Percentage']
            )
            fig.update_traces(
                hovertemplate='<b>%{label}</b><br>Count: %{customdata[0]}<br>Percentage: %{customdata[1]:.1f}%'
            )
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Device category information is not available.")
    
    # Sentiment distribution
    with col2:
        st.subheader("Sentiment Categories")
        if 'sentiment_category' in df_filtered.columns:
            sentiment_counts = df_filtered['sentiment_category'].value_counts()
            sentiment_percentages = df_filtered['sentiment_category'].value_counts(normalize=True) * 100
            
            sentiment_data = pd.DataFrame({
                'Sentiment': sentiment_counts.index,
                'Count': sentiment_counts.values,
                'Percentage': sentiment_percentages.values
            })
            
            # Define color map for sentiment categories
            sentiment_colors = {
                'Very Positive': '#1B9E77',
                'Positive': '#7FC97F',
                'Neutral': '#BEAED4',
                'Negative': '#FDC086',
                'Very Negative': '#E41A1C'
            }
            
            available_sentiments = sentiment_data['Sentiment'].unique()
            color_sequence = [sentiment_colors.get(s, '#CCCCCC') for s in available_sentiments]
            
            fig = px.pie(
                sentiment_data,
                values='Count',
                names='Sentiment',
                hole=0.4,
                labels={'Sentiment': 'Sentiment Category'},
                hover_data=['Percentage'],
                custom_data=['Count', 'Percentage'],
                color='Sentiment',
                color_discrete_map=sentiment_colors
            )
            fig.update_traces(
                hovertemplate='<b>%{label}</b><br>Count: %{customdata[0]}<br>Percentage: %{customdata[1]:.1f}%'
            )
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Sentiment category information is not available.")
    
    # Resolution distribution
    with col3:
        st.subheader("Resolution Categories")
        if 'resolution' in df_filtered.columns:
            resolution_counts = df_filtered['resolution'].value_counts()
            resolution_percentages = df_filtered['resolution'].value_counts(normalize=True) * 100
            
            resolution_data = pd.DataFrame({
                'Resolution': resolution_counts.index,
                'Count': resolution_counts.values,
                'Percentage': resolution_percentages.values
            })
            
            # Define color map for resolution categories
            resolution_colors = {
                'Resolved': '#4DAF4A',
                'Partial': '#FFFF33',
                'Unresolved': '#E41A1C'
            }
            
            fig = px.pie(
                resolution_data,
                values='Count',
                names='Resolution',
                hole=0.4,
                labels={'Resolution': 'Resolution Category'},
                hover_data=['Percentage'],
                custom_data=['Count', 'Percentage'],
                color='Resolution',
                color_discrete_map=resolution_colors
            )
            fig.update_traces(
                hovertemplate='<b>%{label}</b><br>Count: %{customdata[0]}<br>Percentage: %{customdata[1]:.1f}%'
            )
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Resolution information is not available.")
    
    # Service rating statistics
    st.subheader("Service Rating Statistics")
    
    if 'service_rating_numeric' in df_filtered.columns:
        # Calculate statistics
        valid_ratings = df_filtered['service_rating_numeric'].dropna()
        
        if not valid_ratings.empty:
            rating_mean = valid_ratings.mean()
            rating_median = valid_ratings.median()
            
            try:
                rating_mode = mode(valid_ratings)
            except:
                rating_mode = valid_ratings.value_counts().index[0]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Mean Rating", f"{rating_mean:.2f}")
            
            with col2:
                st.metric("Median Rating", f"{rating_median:.2f}")
            
            with col3:
                st.metric("Mode Rating", f"{rating_mode:.1f}")
            
            # Histogram of service ratings
            fig = px.histogram(
                df_filtered,
                x='service_rating_numeric',
                nbins=10,
                labels={'service_rating_numeric': 'Service Rating', 'count': 'Frequency'},
                title="Distribution of Service Ratings",
                color_discrete_sequence=['#636EFA']
            )
            fig.update_layout(bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No valid service rating data available.")
    else:
        st.warning("Service rating information is not available.")
    
    # Service Index vs Resolution visualization
    st.subheader("Service Index by Resolution Category")
    
    if 'service_index' in df_filtered.columns and 'resolution' in df_filtered.columns:
        # Calculate average service index by resolution
        service_by_resolution = df_filtered.groupby('resolution')['service_index'].mean().reset_index()
        
        # Create bar chart
        fig = px.bar(
            service_by_resolution,
            x='resolution',
            y='service_index',
            color='resolution',
            labels={'resolution': 'Resolution', 'service_index': 'Avg Service Index'},
            color_discrete_map={
                'Resolved': '#4DAF4A',
                'Partial': '#FFFF33',
                'Unresolved': '#E41A1C'
            }
        )
        fig.update_layout(yaxis_range=[0, 10])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Service index or resolution information is not available.")
    
    # Record viewer section
    st.header("Transcript Record Viewer")
    
    # Pagination for records
    records_per_page = 5
    total_records = len(df_filtered)
    total_pages = (total_records + records_per_page - 1) // records_per_page
    
    col1, col2 = st.columns([3, 1])
    with col1:
        page_number = st.slider("Page", 1, max(total_pages, 1), 1)
    with col2:
        st.write(f"Total Records: {total_records}")
    
    # Calculate start and end indices
    start_idx = (page_number - 1) * records_per_page
    end_idx = min(start_idx + records_per_page, total_records)
    
    # Display records for current page
    for idx in range(start_idx, end_idx):
        record = df_filtered.iloc[idx]
        
        with st.expander(f"Record #{idx + 1}: {record.get('conversation_id', 'N/A')} - {record.get('start_time', 'N/A')} - {record.get('agent_name', 'N/A')}"):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("### Transcript")
                if 'transcript' in record:
                    st.text_area("", record['transcript'], height=200, key=f"transcript_{idx}")
                else:
                    st.write("Transcript not available")
                
                st.markdown("### Summary")
                if 'transcript_summary' in record:
                    st.write(record['transcript_summary'])
                else:
                    st.write("Summary not available")
                
                st.markdown("### Main Issue")
                if 'main_issue_answer' in record:
                    st.write(record['main_issue_answer'])
                else:
                    st.write("Main issue not available")
            
            with col2:
                metrics_data = [
                    {"label": "Sentiment Score", "value": f"{record.get('sentiment_score', 'N/A')}", "category": record.get('sentiment_category', 'N/A')},
                    {"label": "Service Rating", "value": f"{record.get('service_rating_numeric', 'N/A')}/10", "category": "Rating"},
                    {"label": "Resolution", "value": record.get('resolution', 'N/A'), "reason": record.get('resolution_reason', 'N/A')},
                    {"label": "Device Category", "value": record.get('device_category', 'N/A')},
                    {"label": "Service Index", "value": f"{record.get('service_index', 'N/A')}/10"},
                    {"label": "Duration", "value": f"{record.get('duration_minutes', 'N/A'):.1f} min" if 'duration_minutes' in record else 'N/A'}
                ]
                
                for metric in metrics_data:
                    st.metric(
                        metric["label"], 
                        metric["value"],
                        delta=metric.get("category") if "category" in metric else None,
                        delta_color="off" if "category" in metric else "normal"
                    )
                
                if 'resolution_reason' in record:
                    st.markdown("### Resolution Reason")
                    st.write(record['resolution_reason'])
                
                if 'service_rating_reason' in record:
                    st.markdown("### Rating Reason")
                    st.write(record['service_rating_reason']) 