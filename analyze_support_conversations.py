import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from load_support_conversations import load_support_conversations

def analyze_support_data(df):
    """
    Perform various analyses on the support conversations data
    """
    print("\n===== SUPPORT CONVERSATIONS ANALYSIS =====\n")
    
    # Basic information about the dataset
    print(f"Total conversations: {len(df)}")
    
    # 1. SENTIMENT ANALYSIS
    print("\n----- Sentiment Distribution -----")
    sentiment_counts = df['SENTIMENT'].value_counts()
    print(sentiment_counts)
    
    # Visualize sentiment distribution
    plt.figure(figsize=(10, 6))
    sns.countplot(x='SENTIMENT', data=df, palette='viridis')
    plt.title('Distribution of Conversation Sentiments')
    plt.savefig('sentiment_distribution.png')
    print("Sentiment distribution visualization saved to 'sentiment_distribution.png'")
    
    # 2. ISSUE RESOLUTION ANALYSIS
    print("\n----- Issue Resolution Analysis -----")
    resolution_counts = df['ISSUE_RESOLVED'].value_counts()
    resolution_percentage = (resolution_counts / len(df)) * 100
    print(f"Issues resolved: {resolution_percentage[True]:.2f}%")
    print(f"Issues not resolved: {resolution_percentage[False]:.2f}%")
    
    # 3. DEVICE TYPES ANALYSIS
    print("\n----- Device Types Analysis -----")
    device_counts = df['DEVICE_NAME'].value_counts().head(10)
    print("Top 10 devices with support conversations:")
    print(device_counts)
    
    # 4. SENTIMENT BY DEVICE
    print("\n----- Sentiment by Device Type -----")
    sentiment_by_device = pd.crosstab(df['DEVICE_NAME'], df['SENTIMENT'])
    print(sentiment_by_device.head(10))
    
    # 5. AGENT PERFORMANCE
    print("\n----- Agent Performance Analysis -----")
    # Resolution rate by agent
    agent_performance = df.groupby('AGENT_ID').agg({
        'CONVERSATION_ID': 'count',
        'ISSUE_RESOLVED': 'mean'
    }).rename(columns={
        'CONVERSATION_ID': 'Total_Conversations',
        'ISSUE_RESOLVED': 'Resolution_Rate'
    }).sort_values('Resolution_Rate', ascending=False)
    
    print("Agent performance by resolution rate:")
    print(agent_performance)
    
    # 6. CONVERSATION DURATION ANALYSIS
    print("\n----- Conversation Duration Analysis -----")
    # Calculate duration in minutes
    df['DURATION_MINUTES'] = (df['END_TIME'] - df['START_TIME']).dt.total_seconds() / 60
    
    print(f"Average conversation duration: {df['DURATION_MINUTES'].mean():.2f} minutes")
    print(f"Minimum duration: {df['DURATION_MINUTES'].min():.2f} minutes")
    print(f"Maximum duration: {df['DURATION_MINUTES'].max():.2f} minutes")
    
    # 7. DURATION VS RESOLUTION
    print("\n----- Duration vs Resolution -----")
    resolution_duration = df.groupby('ISSUE_RESOLVED')['DURATION_MINUTES'].mean()
    print("Average duration by resolution status:")
    print(resolution_duration)
    
    # 8. COMMON ISSUES ANALYSIS
    print("\n----- Common Issues Analysis -----")
    common_issues = df['COMMON_ISSUE'].value_counts().head(10)
    print("Top 10 common issues:")
    print(common_issues)
    
    return df

if __name__ == "__main__":
    # Load the data
    print("Loading support conversations data...")
    df = load_support_conversations()
    
    if df is not None:
        # Add additional columns for analysis
        df['DURATION_MINUTES'] = (df['END_TIME'] - df['START_TIME']).dt.total_seconds() / 60
        
        # Perform analysis
        analyze_support_data(df)
        
        # Save processed DataFrame to CSV for easier access
        df.to_csv('support_conversations.csv', index=False)
        print("\nDataFrame saved to 'support_conversations.csv'")
        
        print("\nAnalysis complete!") 