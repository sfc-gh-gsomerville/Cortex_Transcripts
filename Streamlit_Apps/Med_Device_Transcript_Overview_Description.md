# Med_Device_Transcripts_Overview Application

## Overview

The Med_Device_Transcripts_Overview.py is a Streamlit application designed for analyzing and visualizing customer support transcripts for medical devices. The application provides a comprehensive dashboard for monitoring, analyzing, and exploring transcript data with a focus on service quality metrics, sentiment analysis, and agent performance.

Key features include:
- Interactive filtering of transcript data by date, agent, sentiment, device category, and more
- Visualization of key metrics through charts, graphs, and tables
- Detailed agent performance analysis
- Individual transcript record viewing with comprehensive metrics

## Application Structure

The application is organized into different sections, each serving a specific purpose:

### 1. Setup and Configuration

```python
import streamlit as st
import pandas as pd
import plotly.express as px
```

This section imports necessary libraries and sets up the Streamlit page configuration:
- Streamlit for the web application framework
- Pandas for data manipulation
- Plotly Express for interactive visualizations
- Snowflake connection for database access

### 2. Data Loading and Connection

```python
@st.cache_data(ttl=600)
def load_data():
    # Snowflake connection and data loading
```

This section handles:
- Establishing a connection to the Snowflake database
- Loading transcript data from the TRANSCRIPT_ANALYSIS_RESULTS_FINAL table
- Converting data types (dates, numeric ratings)
- Calculating duration in minutes
- Error handling for database connections

### 3. Data Filtering and Sidebar Controls

The application provides robust filtering capabilities through the sidebar:
- Date range selection
- Agent filtering
- Sentiment category filtering
- Device category filtering
- Resolution status filtering
- Service rating range filtering

These filters modify the displayed data across all tabs of the application.

### 4. Service Index Calculation

```python
def calculate_service_index(row):
    # Calculate service index based on resolution and ratings
```

This function calculates a composite service quality index for each transcript record:
- Resolution component (20% weight): Based on resolution status (Resolved, Partial, Unresolved)
- Service rating component (80% weight): Based on customer service rating
- Returns a value from 0-10 representing overall service quality

### 5. Tab 1: Overview Dashboard

The Overview tab provides high-level metrics and visualizations:

**Key Metrics Display:**
- Total number of transcripts
- Average sentiment score
- Average service rating
- Average service index

**Visualizations:**
- Calls per day trend line
- Device category distribution (pie chart)
- Sentiment category distribution (pie chart)
- Resolution category distribution (pie chart)
- Service rating statistics (mean, median, mode)
- Service rating distribution (histogram)
- Service index by resolution (bar chart)

This tab gives management a quick overview of the current state of customer support.

### 6. Tab 2: Agent Metrics

The Agent Metrics tab focuses on individual agent performance:

**Agent Performance Table:**
- Summary statistics for each agent (total transcripts, sentiment scores, service ratings, etc.)

**Visualizations:**
- Resolution rates by agent (stacked bar chart)
- Sentiment breakdown by agent (stacked bar chart)
- Service rating by agent (horizontal bar chart)
- Service index by agent (horizontal bar chart)
- Device categories handled by agent (heatmap)

This tab enables comparison between agents and identification of strengths and areas for improvement.

### 7. Tab 3: Record Viewer

The Record Viewer tab allows detailed exploration of individual transcript records:

**Features:**
- Date range selection for filtering records
- Expandable record views showing complete transcript details
- Summary of each transcript
- Main issue identification
- Key metrics for each transcript (device category, duration, resolution, etc.)
- Full transcript text
- Resolution and rating reasons

This tab is useful for diving into specific customer interactions and understanding context behind metrics.

## Data Integration

The application integrates with Snowflake for data storage and retrieval, pulling transcript data that has been processed through sentiment analysis and categorization. The data includes:

- Basic transcript metadata (conversation ID, agent name, timestamps)
- Transcript content and summary
- Sentiment scores and categories
- Service ratings and feedback
- Resolution status and reasons
- Device categories

## User Experience

The application is designed for ease of use with:
- Responsive layout that adapts to different screen sizes
- Expandable sections to manage information density
- Interactive charts with hover details
- Consistent color coding (sentiments, resolutions)
- Clear organization of information across tabs

## Technical Implementation Notes

The application implements several technical best practices:
- Data caching to improve performance
- Error handling for database connections and data processing
- Responsive design elements
- Flexible query options for different database configurations
- Debug information for troubleshooting 