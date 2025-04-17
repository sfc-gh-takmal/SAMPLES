import _snowflake
import json
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# --- Configurable Variables ---
DB = "CORTEX_ANALYST_DEMO"
SCHEMA = "REVENUE_TIMESERIES"
STAGE = "RAW_DATA"
FILE_NAME = "revenue_timeseries.yaml"
STAGE_PATH = f"@{DB}.{SCHEMA}.{STAGE}/{FILE_NAME}"

# Get session
session = get_active_session()

# Query Cortex Analyst logs
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

# Extract plain text from RESPONSE_BODY
def extract_text_response(response):
    try:
        content = json.loads(response)['message']['content']
        for item in content:
            if isinstance(item, dict) and 'text' in item:
                return item['text']
    except:
        return None

df['TEXT_RESPONSE'] = df['RESPONSE_BODY'].apply(extract_text_response)

# --- Intelligence: Intent Classification ---
def classify_intent(question):
    q = str(question).lower()
    if any(word in q for word in ['trend', 'over time', 'daily', 'weekly', 'monthly']):
        return 'Trend Analysis'
    if any(word in q for word in ['average', 'sum', 'count', 'aggregate']):
        return 'Aggregation'
    if ' vs ' in q or 'compare' in q:
        return 'Comparison'
    if 'anomaly' in q or 'unexpected' in q:
        return 'Anomaly Detection'
    return 'Other'

df['INTENT'] = df['LATEST_QUESTION'].apply(classify_intent)

# --- Intelligence: Query Complexity Score ---
def query_complexity(sql):
    if pd.isna(sql):
        return 0
    return sql.count("SELECT") + sql.count("JOIN") + sql.count("WITH")

df['COMPLEXITY_SCORE'] = df['GENERATED_SQL'].apply(query_complexity)

# --- Sidebar Filters ---
st.sidebar.title("Filters")

# User filter
selected_user = st.sidebar.selectbox("User", ["All"] + sorted(df['USER_NAME'].unique().tolist()))
if selected_user != "All":
    df = df[df['USER_NAME'] == selected_user]

# Semantic model filter
selected_model = st.sidebar.selectbox("Semantic Model", ["All"] + sorted(df['SEMANTIC_MODEL_NAME'].unique().tolist()))
if selected_model != "All":
    df = df[df['SEMANTIC_MODEL_NAME'] == selected_model]

# Date range filter
start_date = st.sidebar.date_input("Start Date", df['TIMESTAMP'].min().date())
end_date = st.sidebar.date_input("End Date", df['TIMESTAMP'].max().date())
df = df[(df['TIMESTAMP'].dt.date >= start_date) & (df['TIMESTAMP'].dt.date <= end_date)]

# Keyword search
search_term = st.sidebar.text_input("Search (Question or SQL)")
if search_term:
    df = df[df['LATEST_QUESTION'].str.contains(search_term, case=False, na=False) |
            df['GENERATED_SQL'].str.contains(search_term, case=False, na=False)]

# --- Title ---
st.title("📊 Snowflake Cortex Analyst Dashboard")

# --- KPIs ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Requests", len(df))
col2.metric("Successful Requests", df['GENERATED_SQL'].notnull().sum())
col3.metric("Failure Rate", f"{100 * (df['GENERATED_SQL'].isnull().sum() / len(df)):.1f}%")

# --- Daily Usage Trend ---
st.subheader("📅 Daily Usage Trend")
daily_counts = df.groupby(df['TIMESTAMP'].dt.date).size().reset_index(name='total_requests')
st.line_chart(daily_counts.rename(columns={'TIMESTAMP': 'Date'}).set_index('Date'))

# --- Intent Distribution ---
st.subheader("🧠 Question Intent Distribution")
st.bar_chart(df['INTENT'].value_counts())

# --- Questions with SQL ---
st.subheader("🙋 Questions by User with Generated SQL")
questions_df = df[[
    'TIMESTAMP', 'REQUEST_ID', 'USER_NAME', 'LATEST_QUESTION', 'TEXT_RESPONSE', 'GENERATED_SQL', 'INTENT', 'COMPLEXITY_SCORE'
]].rename(columns={'LATEST_QUESTION': 'QUESTION'})
questions_df = questions_df.sort_values(by='TIMESTAMP', ascending=False)
st.dataframe(questions_df)

# --- Table Usage ---
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

# --- Requests with Warnings ---
st.subheader("⚠️ Requests with Warnings")
with st.expander("View Requests with Warnings"):
    warnings_df = df[
        df['WARNINGS'].notna() &
        (df['WARNINGS'].str.lower() != 'null') &
        (df['WARNINGS'] != '[]')
    ][[
        'TIMESTAMP', 'USER_NAME', 'LATEST_QUESTION', 'WARNINGS'
    ]].rename(columns={'LATEST_QUESTION': 'QUESTION'})
    st.dataframe(warnings_df)

# --- Top Warning Patterns ---
st.subheader("🚨 Top Warning Patterns")
warning_patterns = df['WARNINGS'][
    df['WARNINGS'].notna() &
    (df['WARNINGS'].str.lower() != 'null') &
    (df['WARNINGS'] != '[]')
].value_counts().head(10)
st.bar_chart(warning_patterns)

# --- Failed Requests ---
st.subheader("🚫 Failed Requests")
with st.expander("View Failed Requests"):
    failed_df = df[df['GENERATED_SQL'].isnull()][[
        'TIMESTAMP', 'USER_NAME', 'LATEST_QUESTION', 'RESPONSE_STATUS_CODE', 'WARNINGS'
    ]].rename(columns={'LATEST_QUESTION': 'QUESTION'})
    st.dataframe(failed_df)

# --- Requests Per User ---
st.subheader("👥 Requests Per User")
st.bar_chart(df['USER_NAME'].value_counts())

# --- Success vs Failure by User ---
st.subheader("✅ Success vs ❌ Failure by User")
df['SUCCESS'] = df['GENERATED_SQL'].notna()
success_df = df.groupby(['USER_NAME', 'SUCCESS']).size().unstack(fill_value=0)
success_df['Total'] = success_df.sum(axis=1)
success_df = success_df.sort_values(by='Total', ascending=False)
st.bar_chart(success_df.drop(columns='Total'))

# --- User Activity Table ---
st.subheader("🔍 User Activity Table")
user_counts = df['USER_NAME'].value_counts().reset_index()
user_counts.columns = ['USER_NAME', 'TOTAL_REQUESTS']
st.dataframe(user_counts)

# --- Feedback Summary ---
st.subheader("💬 Feedback Summary")
with st.expander("View Feedback"):
    feedback_df = df[df['FEEDBACK'].notna() & (df['FEEDBACK'] != '[]')][[
        'TIMESTAMP', 'USER_NAME', 'LATEST_QUESTION', 'FEEDBACK'
    ]].rename(columns={'LATEST_QUESTION': 'QUESTION'})
    st.dataframe(feedback_df)