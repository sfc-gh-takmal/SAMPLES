import streamlit as st
import pandas as pd
import json
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import StructType, StructField, StringType
import _snowflake                     # Snowflake-internal HTTP helper
from datetime import datetime         # Added for timestamp

# ──────────── PAGE CONFIG ───────────────────────────────────────────
st.set_page_config(page_title="Cortex Analyst Batch Tester", layout="wide")

# Initialize session state
if 'results_df' not in st.session_state:
    st.session_state.results_df = None
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

st.title("🧪 Cortex Analyst Batch Test App")
st.markdown(
"""
Provide the fully-qualified **stage path** to your semantic-model YAML
and one or more natural-language **questions** (each on its own line).

For every question we will:

1. Call **Cortex Analyst** and capture the *interpretation* and any follow-ups.  
2. Extract the generated SQL.  
3. **Execute** the SQL with Snowpark (applying a preview limit safely).  
4. Record the **Query ID** and **Request ID**.  
5. Show everything in one dataframe and let you download it as CSV.
"""
)

# ──────────── HELPER FUNCTIONS ──────────────────────────────────────
def call_cortex(question: str, sm_path: str):
    """Call Cortex Analyst. Returns interpretation, follow_up, sql, request_id."""
    body = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": question}]}
        ],
        "semantic_model_file": f"@{sm_path}"
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {}, {}, body, {}, 30000
    )

    if resp["status"] >= 400:
        raise RuntimeError(f"Cortex Analyst error {resp['status']}: {resp}")

    parsed = json.loads(resp["content"])
    interpretation = follow_up = sql_stmt = ""
    for part in parsed["message"]["content"]:
        if part["type"] == "text":            # Analyst's natural-language interpretation
            interpretation = part.get("text", "")
        elif part["type"] == "suggestions":   # follow-ups (comma-joined)
            follow_up = ", ".join(part.get("suggestions", []))
        elif part["type"] == "sql":           # generated SQL
            sql_stmt = part.get("statement", "")

    request_id = parsed.get("request_id", "Unknown")
    return interpretation, follow_up, sql_stmt, request_id


def execute_sql(sql: str, limit_rows: int):
    """
    Run SQL via Snowpark *without* string-hacking the LIMIT.
    Returns (preview_rows:list[dict], query_id:str)
    """
    if not sql.strip():
        return [], "N/A"

    session = get_active_session()

    # Strip a trailing semicolon if present (Snowpark dislikes it)
    sql_clean = sql.rstrip().rstrip(";")

    try:
        df_sp = session.sql(sql_clean)
        df_preview = df_sp.limit(limit_rows)
        preview_rows = df_preview.to_pandas().to_dict(orient="records")
    except Exception as exc:
        return [{"error": str(exc)}], "N/A"

    query_id = session.sql("SELECT LAST_QUERY_ID() AS QID").collect()[0][0]
    return preview_rows, query_id


@st.cache_data
def get_databases():
    """Get list of available databases."""
    try:
        session = get_active_session()
        databases = session.sql("SHOW DATABASES").collect()
        return [row['name'] for row in databases]
    except Exception:
        return []


@st.cache_data
def get_schemas(database: str):
    """Get list of schemas for a given database."""
    try:
        session = get_active_session()
        schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
        return [row['name'] for row in schemas]
    except Exception:
        return []


@st.cache_data
def get_tables(database: str, schema: str):
    """Get list of tables for a given database and schema."""
    try:
        session = get_active_session()
        tables = session.sql(f"SHOW TABLES IN SCHEMA {database}.{schema}").collect()
        return [row['name'] for row in tables]
    except Exception:
        return []


def save_to_snowflake(df: pd.DataFrame, database: str, schema: str, table: str, mode: str):
    """Save dataframe to Snowflake table."""
    try:
        session = get_active_session()
        
        # Convert pandas DataFrame to Snowpark DataFrame
        snowpark_df = session.create_dataframe(df)
        
        full_table_name = f"{database}.{schema}.{table}"
        
        if mode == "create_new":
            # Create new table (will fail if exists)
            snowpark_df.write.save_as_table(full_table_name, mode="errorifexists")
            return f"✅ Successfully created new table: {full_table_name}"
            
        elif mode == "replace":
            # Replace existing table
            snowpark_df.write.save_as_table(full_table_name, mode="overwrite")
            return f"✅ Successfully replaced table: {full_table_name}"
            
        elif mode == "append":
            # Append to existing table
            snowpark_df.write.save_as_table(full_table_name, mode="append")
            return f"✅ Successfully appended to table: {full_table_name}"
            
    except Exception as e:
        return f"❌ Error saving to Snowflake: {str(e)}"


# ──────────── SIDEBAR ───────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Options")
    row_limit = st.number_input(
        "Preview rows per query",
        min_value=1, max_value=100, value=3, step=2
    )
    
    # Show download and save options only if results exist
    if st.session_state.results_df is not None:
        st.divider()
        
        # CSV Download
        st.subheader("⬇️ Download")
        st.download_button(
            "Download CSV",
            data=st.session_state.results_df.to_csv(index=False).encode(),
            file_name="cortex_analyst_batch_results.csv",
            mime="text/csv",
            use_container_width=True
        )
        
        # Snowflake Save Section
        st.divider()
        st.subheader("💾 Save to Snowflake")
        
        # Database selection
        databases = get_databases()
        if databases:
            selected_db = st.selectbox(
                "Database:",
                options=["Select a database..."] + databases,
                key="sf_db_select"
            )
            
            # Schema selection
            if selected_db and selected_db != "Select a database...":
                schemas = get_schemas(selected_db)
                if schemas:
                    selected_schema = st.selectbox(
                        "Schema:",
                        options=["Select a schema..."] + schemas,
                        key=f"sf_schema_select_{selected_db}"
                    )
                    
                    # Table selection and mode
                    if selected_schema and selected_schema != "Select a schema...":
                        tables = get_tables(selected_db, selected_schema)
                        
                        # Mode selection
                        save_mode = st.radio(
                            "Save mode:",
                            options=["create_new", "replace", "append"],
                            format_func=lambda x: {
                                "create_new": "Create new table",
                                "replace": "Replace existing table", 
                                "append": "Append to existing table"
                            }[x],
                            key=f"sf_save_mode_{selected_db}_{selected_schema}"
                        )
                        
                        # Table name input
                        if save_mode == "create_new":
                            table_name = st.text_input(
                                "New table name:",
                                placeholder="cortex_analyst_results",
                                key=f"sf_new_table_{selected_db}_{selected_schema}"
                            )
                        else:
                            if tables:
                                table_name = st.selectbox(
                                    "Existing table:",
                                    options=["Select a table..."] + tables,
                                    key=f"sf_existing_table_{selected_db}_{selected_schema}"
                                )
                                if table_name == "Select a table...":
                                    table_name = ""
                            else:
                                st.warning("No tables found in this schema.")
                                table_name = ""
                        
                        # Save button
                        if table_name:
                            if st.button(
                                "💾 Save to Snowflake", 
                                type="primary", 
                                key=f"sf_save_btn_{selected_db}_{selected_schema}_{table_name}",
                                use_container_width=True
                            ):
                                with st.spinner("Saving to Snowflake..."):
                                    result_msg = save_to_snowflake(
                                        st.session_state.results_df, 
                                        selected_db, 
                                        selected_schema, 
                                        table_name, 
                                        save_mode
                                    )
                                    if "✅" in result_msg:
                                        st.success(result_msg)
                                    else:
                                        st.error(result_msg)
                else:
                    st.warning("No schemas found in this database.")
        else:
            st.warning("No databases accessible or error retrieving databases.")

# ──────────── MAIN INPUT FIELDS ─────────────────────────────────────
semantic_model_path = st.text_input(
    "Semantic-model YAML path",
    placeholder="MYDB.MYSCHEMA.MYSTAGE/model.yaml"
)

questions_input = st.text_area(
    "Questions (one per line)",
    height=200,
    placeholder="e.g.\nWhat were total online sales yesterday?\nTop 5 products by margin this month"
)

# Clear results button (only show if results exist)
col1, col2 = st.columns([1, 6])
with col1:
    if st.session_state.results_df is not None:
        if st.button("🗑️ Clear Results"):
            st.session_state.results_df = None
            st.session_state.processing_complete = False
            st.rerun()

with col2:
    run_btn = st.button("🚀 Run tests", type="primary")

# ──────────── MAIN PROCESSING ───────────────────────────────────────
if run_btn:
    if not semantic_model_path.strip():
        st.error("Please provide a semantic-model path.")
        st.stop()
    if not questions_input.strip():
        st.error("Please enter at least one question.")
        st.stop()

    # Capture the timestamp when Run tests button is pressed
    run_timestamp = datetime.now()

    questions = [q.strip() for q in questions_input.splitlines() if q.strip()]
    results = []
    prog = st.progress(0, text="Starting…")

    for idx, q in enumerate(questions, start=1):
        try:
            prog.progress((idx - 0.5)/len(questions),
                          text=f"Analyst {idx}/{len(questions)}")
            interp, follow_up, sql, req_id = call_cortex(q, semantic_model_path)

            prog.progress(idx/len(questions),
                          text=f"Running SQL {idx}/{len(questions)}")
            preview, qid = execute_sql(sql, row_limit)
            preview_str = json.dumps(preview, default=str) if preview else "No rows"

        except Exception as err:
            interp = f"ERROR → {err}"
            follow_up = sql = preview_str = ""
            qid = req_id = "N/A"

        results.append({
            "created_at": run_timestamp,  # Add timestamp to each result
            "question": q,
            "interpretation": interp,
            "follow_up": follow_up,
            "query": sql,
            "result_preview": preview_str,
            "query_id": qid,
            "request_id": req_id,
        })

    prog.empty()
    
    # Store results in session state
    st.session_state.results_df = pd.DataFrame(results)
    st.session_state.processing_complete = True
    st.rerun()  # Refresh to show the sidebar options

# ──────────── DISPLAY RESULTS ───────────────────────────────────────
if st.session_state.results_df is not None:
    st.subheader(f"✅ Results ({len(st.session_state.results_df)})")
    st.dataframe(st.session_state.results_df, use_container_width=True)
    
    if st.session_state.processing_complete:
        st.success("All questions processed! Use the sidebar to download or save to Snowflake.")
        st.session_state.processing_complete = False  # Reset flag