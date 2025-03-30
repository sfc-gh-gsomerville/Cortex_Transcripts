import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from load_support_conversations import load_support_conversations

def filter_and_analyze_support_data(df, device_type=None, sentiment=None, agent_id=None):
    """
    Filter and analyze support conversations based on specified criteria
    
    Parameters:
    - df: DataFrame containing support conversation data
    - device_type: Filter by specific device type (optional)
    - sentiment: Filter by sentiment type (positive, neutral, negative) (optional)
    - agent_id: Filter by specific agent ID (optional)
    
    Returns:
    - Filtered DataFrame
    """
    filtered_df = df.copy()
    filter_conditions = []
    
    # Build filter description
    filter_desc = "Filtered by: "
    
    if device_type:
        filtered_df = filtered_df[filtered_df['DEVICE_NAME'] == device_type]
        filter_conditions.append(f"Device Type: {device_type}")
    
    if sentiment:
        filtered_df = filtered_df[filtered_df['SENTIMENT'] == sentiment]
        filter_conditions.append(f"Sentiment: {sentiment}")
    
    if agent_id:
        filtered_df = filtered_df[filtered_df['AGENT_ID'] == agent_id]
        filter_conditions.append(f"Agent ID: {agent_id}")
    
    filter_desc += ", ".join(filter_conditions) if filter_conditions else "No filters applied"
    
    print(f"\n===== FILTERED SUPPORT DATA ANALYSIS =====")
    print(filter_desc)
    print(f"Found {len(filtered_df)} matching conversations\n")
    
    if len(filtered_df) == 0:
        print("No data matches the specified filters.")
        return filtered_df
    
    # Basic analysis on filtered data
    
    # 1. Resolution rate
    resolution_rate = filtered_df['ISSUE_RESOLVED'].mean() * 100
    print(f"Resolution rate: {resolution_rate:.2f}%")
    
    # 2. Average conversation duration
    filtered_df['DURATION_MINUTES'] = (filtered_df['END_TIME'] - filtered_df['START_TIME']).dt.total_seconds() / 60
    avg_duration = filtered_df['DURATION_MINUTES'].mean()
    print(f"Average conversation duration: {avg_duration:.2f} minutes")
    
    # 3. Sentiment breakdown (if not filtered by sentiment)
    if not sentiment:
        print("\nSentiment breakdown:")
        sentiment_counts = filtered_df['SENTIMENT'].value_counts()
        for sent, count in sentiment_counts.items():
            percentage = (count / len(filtered_df)) * 100
            print(f"  {sent}: {count} ({percentage:.2f}%)")
    
    # 4. Common issues for filtered data
    print("\nCommon issues in filtered data:")
    common_issues = filtered_df['COMMON_ISSUE'].value_counts().head(5)
    for issue, count in common_issues.items():
        print(f"  - {issue}: {count}")
    
    # 5. Display sample conversation
    if len(filtered_df) > 0:
        print("\nSample conversation:")
        sample = filtered_df.iloc[0]
        print(f"Conversation ID: {sample['CONVERSATION_ID']}")
        print(f"Device: {sample['DEVICE_NAME']}")
        print(f"Issue: {sample['COMMON_ISSUE']}")
        print(f"Sentiment: {sample['SENTIMENT']}")
        print(f"Resolved: {sample['ISSUE_RESOLVED']}")
        
        # Print first 200 characters of transcript with ellipsis if longer
        transcript = sample['TRANSCRIPT']
        if len(transcript) > 200:
            print(f"Transcript preview: {transcript[:200]}...")
        else:
            print(f"Transcript preview: {transcript}")
    
    return filtered_df

def main():
    # Load the data
    print("Loading support conversations data...")
    df = load_support_conversations()
    
    if df is not None:
        # Example 1: Filter by device type
        insulin_pump_data = filter_and_analyze_support_data(df, device_type="Insulin Pump")
        print("\n" + "="*50 + "\n")
        
        # Example 2: Filter by sentiment
        negative_sentiment_data = filter_and_analyze_support_data(df, sentiment="negative")
        print("\n" + "="*50 + "\n")
        
        # Example 3: Filter by agent
        agent_data = filter_and_analyze_support_data(df, agent_id=5)
        print("\n" + "="*50 + "\n")
        
        # Example 4: Combine filters - negative sentiment for Insulin Pump
        combined_filter_data = filter_and_analyze_support_data(
            df, 
            device_type="Insulin Pump", 
            sentiment="negative"
        )
        
        # Example 5: Create visualization for filtered data
        if len(insulin_pump_data) > 0:
            plt.figure(figsize=(10, 6))
            sns.countplot(data=insulin_pump_data, x='ISSUE_RESOLVED', hue='SENTIMENT')
            plt.title('Issue Resolution by Sentiment for Insulin Pump Support Cases')
            plt.xlabel('Issue Resolved')
            plt.ylabel('Count')
            plt.savefig('insulin_pump_resolution_by_sentiment.png')
            print("\nSaved visualization to 'insulin_pump_resolution_by_sentiment.png'")

if __name__ == "__main__":
    main() 
