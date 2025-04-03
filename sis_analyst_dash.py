import _snowflake
import json
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session


# --- Configurable Variables ---
DB = "<your_database_name>"
SCHEMA = "<your_schema_name>"
STAGE = "<your_stage_name>"
FILE_NAME = "<your_file_name>.yaml"
STAGE_PATH = f"@{DB}.{SCHEMA}.{STAGE}/{FILE_NAME}"

# Get session
session = get_active_session()

# Query base Cortex Analyst logs
query = f"""
SELECT *
FROM TABLE(
  SNOWFLAKE.LOCAL.CORTEX_ANALYST_REQUESTS(
    'FILE_ON_STAGE',
    '{STAGE_PATH}'
  )
)
"""
df = session.sql(query).to_pandas()

# Convert timestamp
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])

# Sidebar filters
st.sidebar.title("Filters")
selected_user = st.sidebar.selectbox("Select User", options=["All"] + sorted(df['USER_NAME'].unique().tolist()))

if selected_user != "All":
    df = df[df['USER_NAME'] == selected_user]

# Title
st.title("📊 Snowflake Cortex Analyst Dashboard")

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Total Requests", len(df))
col2.metric("Successful Requests", df['GENERATED_SQL'].notnull().sum())
col3.metric("Failure Rate", f"{100 * (df['GENERATED_SQL'].isnull().sum() / len(df)):.1f}%")

# Daily usage trend
st.subheader("📅 Daily Usage Trend")
daily_counts = df.groupby(df['TIMESTAMP'].dt.date).size().reset_index(name='total_requests')
st.line_chart(daily_counts.rename(columns={'TIMESTAMP': 'Date'}).set_index('Date'))

# Most common questions
st.subheader("💬 Top 10 Most Asked Questions")
question_counts = df['LATEST_QUESTION'].value_counts().head(10)
st.bar_chart(question_counts)

# Table usage
st.subheader("📃 Most Referenced Tables")
def extract_tables(row):
    try:
        return json.loads(row.replace("\n", "").replace("'", '"'))
    except:
        return []

all_tables = df['TABLES_REFERENCED'].dropna().apply(extract_tables)
table_flat = pd.Series([table for sublist in all_tables for table in sublist])
table_counts = table_flat.value_counts().head(10)
st.bar_chart(table_counts)

# Requests with warnings
st.subheader("⚠️ Requests with Warnings")
with st.expander("View Requests with Warnings"):
    st.dataframe(df[df['WARNINGS'].notna() & (df['WARNINGS'] != '[]')][[
        'TIMESTAMP', 'USER_NAME', 'LATEST_QUESTION', 'WARNINGS'
    ]])

# Failures
st.subheader("🚫 Failed Requests")
with st.expander("View Failed Requests"):
    st.dataframe(df[df['GENERATED_SQL'].isnull()][[
        'TIMESTAMP', 'USER_NAME', 'LATEST_QUESTION', 'RESPONSE_STATUS_CODE', 'WARNINGS'
    ]])

# User-level activity
st.subheader("🔍 User Activity")
user_counts = df['USER_NAME'].value_counts().reset_index()
user_counts.columns = ['USER_NAME', 'TOTAL_REQUESTS']
st.dataframe(user_counts)

# Feedback summary
st.subheader("💬 Feedback Summary")
with st.expander("View Feedback"):
    st.dataframe(df[df['FEEDBACK'].notna() & (df['FEEDBACK'] != '[]')][['USER_NAME', 'LATEST_QUESTION', 'FEEDBACK']])
