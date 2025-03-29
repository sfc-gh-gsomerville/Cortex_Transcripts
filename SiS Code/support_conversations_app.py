# Import required packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Support Conversations Analysis",
    page_icon="📞",
    layout="wide"
)

# Set up Snowflake session using get_active_session
session = get_active_session()

# Function to load data from Snowflake
@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_data():
    # Query the support conversations table
    df = session.table("CURSOR_DEMO.V1.SUPPORT_CONVERSATIONS").to_pandas()
    
    # Add derived columns
    df['DURATION_MINUTES'] = (df['END_TIME'] - df['START_TIME']).dt.total_seconds() / 60
    
    return df

# Function to create sentiment distribution chart
def create_sentiment_chart(df):
    sentiment_counts = df['SENTIMENT'].value_counts().reset_index()
    sentiment_counts.columns = ['Sentiment', 'Count']
    
    fig = px.bar(
        sentiment_counts, 
        x='Sentiment', 
        y='Count',
        color='Sentiment',
        color_discrete_map={
            'positive': '#2ecc71',
            'neutral': '#3498db',
            'negative': '#e74c3c'
        },
        title='Distribution of Conversation Sentiments'
    )
    
    fig.update_layout(xaxis_title='Sentiment', yaxis_title='Count')
    return fig

# Function to create resolution chart
def create_resolution_chart(df):
    resolution_counts = df['ISSUE_RESOLVED'].value_counts().reset_index()
    resolution_counts.columns = ['Resolved', 'Count']
    
    # Convert boolean to string for better labels
    resolution_counts['Resolved'] = resolution_counts['Resolved'].map({True: 'Resolved', False: 'Unresolved'})
    
    fig = px.pie(
        resolution_counts, 
        values='Count', 
        names='Resolved',
        color='Resolved',
        color_discrete_map={
            'Resolved': '#2ecc71',
            'Unresolved': '#e74c3c'
        },
        title='Issue Resolution Rate'
    )
    
    return fig

# Function to create agent performance chart
def create_agent_performance_chart(df):
    # Calculate agent performance metrics
    agent_performance = df.groupby('AGENT_ID').agg({
        'CONVERSATION_ID': 'count',
        'ISSUE_RESOLVED': 'mean'
    }).reset_index()
    
    agent_performance.columns = ['Agent ID', 'Total Conversations', 'Resolution Rate']
    agent_performance['Resolution Rate'] = agent_performance['Resolution Rate'] * 100
    
    # Sort by resolution rate descending
    agent_performance = agent_performance.sort_values('Resolution Rate', ascending=False)
    
    fig = px.bar(
        agent_performance,
        x='Agent ID',
        y='Resolution Rate',
        color='Total Conversations',
        color_continuous_scale='viridis',
        title='Agent Performance by Resolution Rate (%)'
    )
    
    fig.update_layout(xaxis_title='Agent ID', yaxis_title='Resolution Rate (%)')
    return fig, agent_performance

# Function to create device types chart
def create_device_types_chart(df):
    device_counts = df['DEVICE_NAME'].value_counts().reset_index()
    device_counts.columns = ['Device', 'Count']
    
    # Get top 10 devices
    top_devices = device_counts.head(10)
    
    fig = px.bar(
        top_devices,
        x='Device',
        y='Count',
        title='Top 10 Devices with Support Conversations'
    )
    
    fig.update_layout(xaxis_title='Device', yaxis_title='Count')
    fig.update_xaxes(tickangle=45)
    return fig

# Function to filter data based on selections
def filter_data(df, sentiment=None, device=None, agent_id=None, date_range=None):
    filtered_df = df.copy()
    
    if sentiment and sentiment != "All":
        filtered_df = filtered_df[filtered_df['SENTIMENT'] == sentiment.lower()]
    
    if device and device != "All":
        filtered_df = filtered_df[filtered_df['DEVICE_NAME'] == device]
    
    if agent_id and agent_id != "All":
        filtered_df = filtered_df[filtered_df['AGENT_ID'] == agent_id]
    
    if date_range:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['START_TIME'].dt.date >= start_date) & 
            (filtered_df['START_TIME'].dt.date <= end_date)
        ]
    
    return filtered_df

# Main application
def main():
    # Add header
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image("https://www.snowflake.com/wp-content/themes/snowflake/assets/img/snowflake-logo-blue.svg", width=100)
    with col2:
        st.title("Support Conversations Analysis")
        st.markdown("Interactive dashboard for analyzing customer support conversations")
    
    # Load data
    with st.spinner("Loading data from Snowflake..."):
        df = load_data()
    
    # Get unique values for filters
    sentiment_options = ["All"] + sorted(df['SENTIMENT'].unique().tolist())
    device_options = ["All"] + sorted(df['DEVICE_NAME'].unique().tolist())
    agent_options = ["All"] + sorted(df['AGENT_ID'].unique().astype(str).tolist())
    
    # Create filters in sidebar
    st.sidebar.header("Filters")
    
    sentiment_filter = st.sidebar.selectbox("Sentiment", sentiment_options)
    device_filter = st.sidebar.selectbox("Device", device_options)
    agent_filter = st.sidebar.selectbox("Agent ID", agent_options)
    
    # Date range filter
    min_date = df['START_TIME'].dt.date.min()
    max_date = df['START_TIME'].dt.date.max()
    
    st.sidebar.subheader("Date Range")
    date_range = st.sidebar.date_input(
        "Select date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Apply filters if both start and end dates are provided
    date_filter = None
    if len(date_range) == 2:
        date_filter = date_range
    
    # Convert agent filter to integer if not "All"
    agent_id_filter = None
    if agent_filter != "All":
        agent_id_filter = int(agent_filter)
    
    # Apply filters
    filtered_df = filter_data(
        df, 
        sentiment=sentiment_filter, 
        device=device_filter, 
        agent_id=agent_id_filter,
        date_range=date_filter
    )
    
    # Add reset filters button
    if st.sidebar.button("Reset Filters"):
        st.experimental_rerun()
    
    # Display basic metrics
    st.subheader("Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Conversations", len(filtered_df))
    
    with col2:
        resolution_rate = filtered_df['ISSUE_RESOLVED'].mean() * 100
        st.metric("Resolution Rate", f"{resolution_rate:.1f}%")
    
    with col3:
        avg_duration = filtered_df['DURATION_MINUTES'].mean()
        st.metric("Avg. Duration", f"{avg_duration:.1f} min")
    
    with col4:
        positive_sentiment = (filtered_df['SENTIMENT'] == 'positive').mean() * 100
        st.metric("Positive Sentiment", f"{positive_sentiment:.1f}%")
    
    # Create main visualization section
    st.subheader("Visualizations")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Sentiment", "Resolution", "Agent Performance", "Device Types"])
    
    with tab1:
        sentiment_chart = create_sentiment_chart(filtered_df)
        st.plotly_chart(sentiment_chart, use_container_width=True)
        
        # Sentiment breakdown table
        sentiment_counts = filtered_df['SENTIMENT'].value_counts().reset_index()
        sentiment_counts.columns = ['Sentiment', 'Count']
        sentiment_counts['Percentage'] = sentiment_counts['Count'] / sentiment_counts['Count'].sum() * 100
        sentiment_counts['Percentage'] = sentiment_counts['Percentage'].round(1).astype(str) + '%'
        
        st.dataframe(sentiment_counts, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            resolution_chart = create_resolution_chart(filtered_df)
            st.plotly_chart(resolution_chart, use_container_width=True)
        
        with col2:
            # Resolution by sentiment
            resolution_by_sentiment = pd.crosstab(
                filtered_df['SENTIMENT'], 
                filtered_df['ISSUE_RESOLVED'],
                normalize='index'
            ) * 100
            
            resolution_by_sentiment.columns = ['Unresolved (%)', 'Resolved (%)']
            resolution_by_sentiment = resolution_by_sentiment.reset_index()
            
            fig = px.bar(
                resolution_by_sentiment,
                x='SENTIMENT',
                y=['Resolved (%)', 'Unresolved (%)'],
                title='Resolution Rate by Sentiment',
                barmode='stack'
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        agent_chart, agent_df = create_agent_performance_chart(filtered_df)
        st.plotly_chart(agent_chart, use_container_width=True)
        
        # Display agent performance table
        st.dataframe(agent_df, use_container_width=True)
        
        # Show agent sentiment breakdown if an agent is selected
        if agent_id_filter:
            agent_sentiment = filtered_df['SENTIMENT'].value_counts(normalize=True) * 100
            
            fig = px.pie(
                values=agent_sentiment.values,
                names=agent_sentiment.index,
                title=f'Sentiment Distribution for Agent {agent_id_filter}',
                color=agent_sentiment.index,
                color_discrete_map={
                    'positive': '#2ecc71',
                    'neutral': '#3498db',
                    'negative': '#e74c3c'
                }
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        device_chart = create_device_types_chart(filtered_df)
        st.plotly_chart(device_chart, use_container_width=True)
        
        # Common issues for the selected device
        if device_filter != "All":
            st.subheader(f"Common Issues for {device_filter}")
            
            issues = filtered_df['COMMON_ISSUE'].value_counts().reset_index()
            issues.columns = ['Issue', 'Count']
            
            st.dataframe(issues, use_container_width=True)
    
    # Conversation samples section
    st.subheader("Conversation Samples")
    
    # Allow searching by conversation ID
    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        search_id = st.number_input("Search by Conversation ID", min_value=1, step=1)
    with search_col2:
        search_button = st.button("Search")
    
    if search_button and search_id:
        conversation = filtered_df[filtered_df['CONVERSATION_ID'] == search_id]
        if not conversation.empty:
            display_conversation(conversation.iloc[0])
        else:
            st.warning(f"Conversation ID {search_id} not found in the filtered data.")
    
    # Show a random sample of conversations
    if st.button("Show Random Conversation Sample"):
        if not filtered_df.empty:
            random_conversation = filtered_df.sample(1).iloc[0]
            display_conversation(random_conversation)
        else:
            st.warning("No conversations match the current filters.")

# Function to display a single conversation
def display_conversation(conversation):
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader(f"Conversation {conversation['CONVERSATION_ID']}")
        st.write(f"**Device:** {conversation['DEVICE_NAME']}")
        st.write(f"**Issue:** {conversation['COMMON_ISSUE']}")
        st.write(f"**Agent ID:** {conversation['AGENT_ID']}")
        st.write(f"**Customer ID:** {conversation['CUSTOMER_ID']}")
        st.write(f"**Sentiment:** {conversation['SENTIMENT']}")
        st.write(f"**Issue Resolved:** {conversation['ISSUE_RESOLVED']}")
        st.write(f"**Duration:** {conversation['DURATION_MINUTES']:.1f} minutes")
        st.write(f"**Start Time:** {conversation['START_TIME']}")
        st.write(f"**End Time:** {conversation['END_TIME']}")
    
    with col2:
        st.subheader("Transcript")
        st.text_area("", conversation['TRANSCRIPT'], height=400)

# Run the main application
if __name__ == "__main__":
    main() 