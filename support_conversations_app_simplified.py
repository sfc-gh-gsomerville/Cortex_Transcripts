# Import required packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
import pandas as pd
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

# Main application
def main():
    # Add header
    col1, col2 = st.columns([1, 5])
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
    
    # Apply filters
    filtered_df = df.copy()
    
    # Apply sentiment filter
    if sentiment_filter != "All":
        filtered_df = filtered_df[filtered_df['SENTIMENT'] == sentiment_filter.lower()]
    
    # Apply device filter
    if device_filter != "All":
        filtered_df = filtered_df[filtered_df['DEVICE_NAME'] == device_filter]
    
    # Apply agent filter
    if agent_filter != "All":
        filtered_df = filtered_df[filtered_df['AGENT_ID'] == int(agent_filter)]
    
    # Apply date filter
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['START_TIME'].dt.date >= start_date) & 
            (filtered_df['START_TIME'].dt.date <= end_date)
        ]
    
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
    st.subheader("Data Analysis")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Sentiment", "Resolution", "Agent Performance", "Device Types"])
    
    with tab1:
        # Sentiment breakdown table
        st.subheader("Sentiment Distribution")
        sentiment_counts = filtered_df['SENTIMENT'].value_counts().reset_index()
        sentiment_counts.columns = ['Sentiment', 'Count']
        sentiment_counts['Percentage'] = (sentiment_counts['Count'] / sentiment_counts['Count'].sum() * 100).round(1)
        
        # Create a simple bar chart with Streamlit
        st.bar_chart(sentiment_counts.set_index('Sentiment')['Count'])
        
        # Show the data
        st.dataframe(sentiment_counts, use_container_width=True)
    
    with tab2:
        # Resolution breakdown
        st.subheader("Issue Resolution")
        
        # Calculate resolution counts
        resolution_counts = filtered_df['ISSUE_RESOLVED'].value_counts()
        resolved = resolution_counts.get(True, 0)
        unresolved = resolution_counts.get(False, 0)
        
        # Create a simple horizontal bar chart
        resolution_data = pd.DataFrame({
            'Status': ['Resolved', 'Unresolved'],
            'Count': [resolved, unresolved]
        })
        
        st.bar_chart(resolution_data.set_index('Status'))
        
        # Resolution by sentiment
        st.subheader("Resolution by Sentiment")
        resolution_by_sentiment = pd.crosstab(
            filtered_df['SENTIMENT'], 
            filtered_df['ISSUE_RESOLVED'],
            normalize='index'
        ) * 100
        
        resolution_by_sentiment.columns = ['Unresolved (%)', 'Resolved (%)']
        
        st.dataframe(resolution_by_sentiment.round(1), use_container_width=True)
    
    with tab3:
        # Agent performance
        st.subheader("Agent Performance")
        
        # Calculate agent performance metrics
        agent_performance = filtered_df.groupby('AGENT_ID').agg({
            'CONVERSATION_ID': 'count',
            'ISSUE_RESOLVED': 'mean'
        }).reset_index()
        
        agent_performance.columns = ['Agent ID', 'Total Conversations', 'Resolution Rate']
        agent_performance['Resolution Rate'] = (agent_performance['Resolution Rate'] * 100).round(1)
        
        # Sort by resolution rate descending
        agent_performance = agent_performance.sort_values('Resolution Rate', ascending=False)
        
        # Display agent performance table
        st.dataframe(agent_performance, use_container_width=True)
        
        # Create a simple bar chart of resolution rates
        st.bar_chart(agent_performance.set_index('Agent ID')['Resolution Rate'])
        
        # Show agent sentiment breakdown if an agent is selected
        if agent_filter != "All":
            st.subheader(f"Sentiment Distribution for Agent {agent_filter}")
            
            agent_sentiment = filtered_df['SENTIMENT'].value_counts(normalize=True) * 100
            agent_sentiment = agent_sentiment.reset_index()
            agent_sentiment.columns = ['Sentiment', 'Percentage']
            agent_sentiment['Percentage'] = agent_sentiment['Percentage'].round(1)
            
            st.dataframe(agent_sentiment, use_container_width=True)
    
    with tab4:
        # Device types analysis
        st.subheader("Device Types")
        
        # Get top 10 devices
        device_counts = filtered_df['DEVICE_NAME'].value_counts().reset_index()
        device_counts.columns = ['Device', 'Count']
        top_devices = device_counts.head(10)
        
        # Create a simple bar chart
        st.bar_chart(top_devices.set_index('Device')['Count'])
        
        # Display the data table
        st.dataframe(top_devices, use_container_width=True)
        
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