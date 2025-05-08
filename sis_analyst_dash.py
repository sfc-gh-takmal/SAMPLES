import _snowflake
import json
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
DB          = "CORTEX_ANALYST_DEMO"
SCHEMA       = "REVENUE_TIMESERIES"
STAGE        = "RAW_DATA"
FILE_NAME    = "revenue_timeseries.yaml"
STAGE_PATH   = f"@{DB}.{SCHEMA}.{STAGE}/{FILE_NAME}"

# Credits charged per 1 000 successful messages for each semantic model
CREDIT_RATE = {
    "default": 67,                 # fallback if model not listed below
    # "MY_MODEL": 72,              # example custom pricing
}

# ──────────────────────────────────────────────────────────────────────────────
# Snowflake session & data load
# ──────────────────────────────────────────────────────────────────────────────
session = get_active_session()

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

if df.empty:
    st.error("No Cortex Analyst log data found. Verify STAGE_PATH or permissions.")
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Data preparation helpers
# ──────────────────────────────────────────────────────────────────────────────

# Parse timestamp column
if "TIMESTAMP" in df.columns:
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])

# Extract plain-text answer from RESPONSE_BODY JSON

def extract_text_response(response: str | None) -> str | None:
    if not isinstance(response, str):
        return None
    try:
        content = json.loads(response)["message"]["content"]
        for item in content:
            if isinstance(item, dict) and "text" in item:
                return item["text"]
    except Exception:
        pass
    return None

df["TEXT_RESPONSE"] = df["RESPONSE_BODY"].apply(extract_text_response)

# Heuristic intent classification

def classify_intent(question: str | None) -> str:
    if not isinstance(question, str):
        return "Other"
    q = question.lower()
    if any(w in q for w in ["trend", "over time", "daily", "weekly", "monthly"]):
        return "Trend Analysis"
    if any(w in q for w in ["average", "sum", "count", "aggregate"]):
        return "Aggregation"
    if " vs " in q or "compare" in q:
        return "Comparison"
    if "anomaly" in q or "unexpected" in q:
        return "Anomaly Detection"
    return "Other"

df["INTENT"] = df["LATEST_QUESTION"].apply(classify_intent)

# Simple query complexity metric

def query_complexity(sql: str | None) -> int:
    if not isinstance(sql, str):
        return 0
    s = sql.upper()
    return s.count("SELECT") + s.count("JOIN") + s.count("WITH")

df["COMPLEXITY_SCORE"] = df["GENERATED_SQL"].apply(query_complexity)

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar filters
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.title("Filters")

selected_user = st.sidebar.selectbox(
    "User",
    ["All"] + sorted(df["USER_NAME"].dropna().unique().tolist())
)
if selected_user != "All":
    df = df[df["USER_NAME"] == selected_user]

selected_model = st.sidebar.selectbox(
    "Semantic Model",
    ["All"] + sorted(df["SEMANTIC_MODEL_NAME"].dropna().unique().tolist())
)
if selected_model != "All":
    df = df[df["SEMANTIC_MODEL_NAME"] == selected_model]

start_date = st.sidebar.date_input("Start Date", df["TIMESTAMP"].min().date())
end_date   = st.sidebar.date_input("End Date", df["TIMESTAMP"].max().date())

df = df[(df["TIMESTAMP"].dt.date >= start_date) & (df["TIMESTAMP"].dt.date <= end_date)]

# Dollar-per-credit input (replaces search box)
dollar_per_credit = st.sidebar.number_input(
    "Dollar Cost per Credit ($)",
    min_value=0.0,
    value=3.00,
    step=0.01,
    format="%.2f",
    help="Enter your negotiated price per Snowflake credit."
)

# ──────────────────────────────────────────────────────────────────────────────
# Dashboard header & KPIs
# ──────────────────────────────────────────────────────────────────────────────
st.title("📊 Snowflake Cortex Analyst Dashboard")

total_requests      = len(df)
successful_requests = df["GENERATED_SQL"].notna().sum()
failure_rate        = 0.0 if total_requests == 0 else 100 * (total_requests - successful_requests) / total_requests

current_rate = CREDIT_RATE.get(selected_model, CREDIT_RATE["default"])
credits_used = successful_requests * current_rate / 1000  # float
cost_estimate = credits_used * dollar_per_credit

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Total Requests", total_requests)
kpi2.metric("Successful Requests", successful_requests)
kpi3.metric("Failure Rate", f"{failure_rate:.1f}%")
kpi4.metric("Est. Credits Used", f"{credits_used:,.2f}")
kpi5.metric("Est. Cost ($)", f"${cost_estimate:,.2f}")

# ──────────────────────────────────────────────────────────────────────────────
# Daily usage trend
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("📅 Daily Usage Trend")
if not df.empty:
    daily_counts = df.groupby(df["TIMESTAMP"].dt.date).size().reset_index(name="total_requests")
    st.line_chart(daily_counts.rename(columns={"TIMESTAMP": "Date"}).set_index("Date"))
else:
    st.info("No data after filters.")

# ──────────────────────────────────────────────────────────────────────────────
# Intent distribution
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("🧠 Question Intent Distribution")
if not df.empty:
    st.bar_chart(df["INTENT"].value_counts())
else:
    st.info("No data to display intent distribution.")

# ──────────────────────────────────────────────────────────────────────────────
# Questions with generated SQL
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("🙋 Questions by User with Generated SQL")
questions_df = df[[
    "TIMESTAMP", "REQUEST_ID", "USER_NAME", "LATEST_QUESTION", "TEXT_RESPONSE",
    "GENERATED_SQL", "INTENT", "COMPLEXITY_SCORE"
]].rename(columns={"LATEST_QUESTION": "QUESTION"}).sort_values("TIMESTAMP", ascending=False)

st.dataframe(questions_df, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# Most referenced tables
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("📃 Most Referenced Tables")

def extract_tables(value: str | None):
    if not isinstance(value, str):
        return []
    try:
        return json.loads(value.replace("\n", "").replace("'", '"'))
    except Exception:
        return []

if df["TABLES_REFERENCED"].notna().any():
    table_series = df["TABLES_REFERENCED"].dropna().apply(extract_tables)
    flat_tables  = pd.Series([t for sub in table_series for t in sub])
    st.bar_chart(flat_tables.value_counts().head(10))
else:
    st.info("No table reference data.")

# ──────────────────────────────────────────────────────────────────────────────
# Requests with warnings
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("⚠️ Requests with Warnings")
with st.expander("Show Warnings"):
    warn_df = df[
        df["WARNINGS"].notna() &
        (df["WARNINGS"].str.lower() != "null") &
        (df["WARNINGS"] != "[]")
    ][["TIMESTAMP", "USER_NAME", "LATEST_QUESTION", "WARNINGS"]].rename(columns={"LATEST_QUESTION": "QUESTION"})
    st.dataframe(warn_df, use_container_width=True)

st.subheader("🚨 Top Warning Patterns")
warning_series = df["WARNINGS"][
    df["WARNINGS"].notna() &
    (df["WARNINGS"].str.lower() != "null") &
    (df["WARNINGS"] != "[]")
]
if not warning_series.empty:
    st.bar_chart(warning_series.value_counts().head(10))
else:
    st.info("No warnings to summarize.")

# ──────────────────────────────────────────────────────────────────────────────
# Failed requests
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("🚫 Failed Requests")
with st.expander("Show Failed Requests"):
    failed_df = df[df["GENERATED_SQL"].isnull()][[
        "TIMESTAMP", "USER_NAME", "LATEST_QUESTION", "RESPONSE_STATUS_CODE", "WARNINGS"
    ]].rename(columns={"LATEST_QUESTION": "QUESTION"})
    st.dataframe(failed_df, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# Requests per user
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("👥 Requests Per User")
if not df.empty:
    st.bar_chart(df["USER_NAME"].value_counts())
else:
    st.info("No user data available.")

# ──────────────────────────────────────────────────────────────────────────────
# Success vs failure by user
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("✅ Success vs ❌ Failure by User")

if not df.empty:
    df["SUCCESS"] = df["GENERATED_SQL"].notna()
    sf_df = df.groupby(["USER_NAME", "SUCCESS"]).size().unstack(fill_value=0)
    sf_df["Total"] = sf_df.sum(axis=1)          # ← axis=1 closes the paren
    st.bar_chart(sf_df.drop(columns="Total"))
else:
    st.info("No success/failure data for selected filters.")