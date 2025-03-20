#------------------------------------------------------------------------------
# IMPORTS
# USE PACKAGE DROPDOWN IN STREAMLIT IN SNOWFLAKE FOR:
# - snowflake-snowpark-python
# - streamlit
#------------------------------------------------------------------------------


import _snowflake
import json
import streamlit as st
import time
import pandas as pd
from snowflake.snowpark.context import get_active_session

DATABASE = "<your_database_name>"
SCHEMA = "<your_schema_name>"
STAGE = "<your_stage_name>"

def get_yaml_files():
    session = get_active_session()
    result = session.sql(f"LIST @{DATABASE}.{SCHEMA}.{STAGE}").collect()
    yaml_files = [row['name'].split('/')[-1] for row in result if row['name'].endswith('.yaml')]
    return yaml_files

def get_yaml_content(file_name):
    session = get_active_session()
    result = session.sql(f"SELECT $1 FROM @{DATABASE}.{SCHEMA}.{STAGE}/{file_name}").collect()
    if result:
        return '\n'.join([row[0] for row in result])
    return "No content found"

def send_message(prompt: str, file: str) -> dict:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{file}",
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )

def process_message(prompt: str, file: str) -> None:
    """Processes a message and adds the response to the chat."""
    # Append the user prompt to session state
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt, file=file)

            # Get the request_id from the API response
            request_id = response.get("request_id", "Unknown")

            # Store the current request ID so display_content can reference it
            st.session_state.current_request_id = request_id

            content = response["message"]["content"]
            display_content(content=content)

            # Show the Request ID (the Query ID is retrieved for each SQL statement below)
            st.write(f"**Request ID:** `{request_id}`")

    # Store the assistant's content + request ID in session state
    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": content,
            "request_id": request_id,
        }
    )

def display_content(content: list, message_index: int = None) -> None:
    """Displays each content item for a message, including running SQL statements."""
    # Use the current number of messages if no explicit index is provided
    message_index = message_index or len(st.session_state.messages)

    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])

        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion

        elif item["type"] == "sql":
            # Show the SQL statement
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")

            # Execute and display results
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    session = get_active_session()
                    df = session.sql(item["statement"]).to_pandas()

                    # Retrieve the actual query ID for *this* SQL statement
                    last_query_id_df = session.sql("SELECT LAST_QUERY_ID() AS QID").collect()
                    last_query_id = last_query_id_df[0][0]

                    # Show the Query ID below the results
                    st.write(f"**Query ID:** `{last_query_id}`")

                    # Append this (request_id, query_id) pair to a global list
                    # so we can display it in the sidebar
                    if "id_pairs" not in st.session_state:
                        st.session_state.id_pairs = []

                    # Use the current_request_id if it exists, otherwise "Unknown"
                    current_req_id = st.session_state.get("current_request_id", "Unknown")

                    st.session_state.id_pairs.append(
                        {
                            "Request Id": current_req_id,
                            "Query Id": last_query_id,
                        }
                    )

                    # Render the data and optional charts
                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab = st.tabs(["Data", "Line Chart", "Bar Chart"])
                        data_tab.dataframe(df, use_container_width=True)
                        if len(df.columns) > 1:
                            df_indexed = df.set_index(df.columns[0])
                        else:
                            df_indexed = df
                        with line_tab:
                            try:
                                st.line_chart(df_indexed)
                            except Exception as e:
                                st.error(f"Could not render line chart: {e}")
                                st.dataframe(df, use_container_width=True)
                        with bar_tab:
                            try:
                                st.bar_chart(df_indexed)
                            except Exception as e:
                                st.error(f"Could not render bar chart: {e}")
                                st.dataframe(df, use_container_width=True)
                    else:
                        st.dataframe(df, use_container_width=True)

########################################
# MAIN APP
########################################

st.title("Cortex Analyst")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None

# Initialize this so we never get an AttributeError
if "current_request_id" not in st.session_state:
    st.session_state.current_request_id = "Unknown"

# If we haven't stored (request_id, query_id) pairs yet, initialize the list
if "id_pairs" not in st.session_state:
    st.session_state.id_pairs = []

# ------------------------------
# SIDEBAR - File selection and YAML
# ------------------------------
st.sidebar.title("File Selection")
yaml_files = get_yaml_files()
selected_file = st.sidebar.selectbox("Select a YAML file", yaml_files)

show_yaml = st.sidebar.checkbox("Show YAML Content", value=False)
if show_yaml:
    st.sidebar.subheader("YAML Content")
    yaml_content = get_yaml_content(selected_file)
    st.sidebar.code(yaml_content, language="yaml", line_numbers=True)

st.markdown(f"Semantic Model: `{selected_file}`")

# Display existing conversation
for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        display_content(content=message["content"], message_index=message_index)

        if message["role"] == "assistant":
            req_id = message.get("request_id", "Unknown")
            st.write(f"**Request ID:** `{req_id}`")

# Handle chat input
if user_input := st.chat_input("What is your question?"):
    process_message(prompt=user_input, file=selected_file)

# Handle suggestions
if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion, file=selected_file)
    st.session_state.active_suggestion = None

# ------------------------------
# Update sidebar with (Request ID, Query ID) pairs
# ------------------------------
if st.session_state.id_pairs:
    st.sidebar.subheader("Request & Query IDs")
    df_ids = pd.DataFrame(st.session_state.id_pairs)
    st.sidebar.dataframe(df_ids)