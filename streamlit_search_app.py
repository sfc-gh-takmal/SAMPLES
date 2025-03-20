#------------------------------------------------------------------------------
# IMPORTS
# USE PACKAGE DROPDOWN IN STREAMLIT IN SNOWFLAKE FOR:
# - snowflake.core
# - snowflake-snowpark-python
# - streamlit
#------------------------------------------------------------------------------

import json
import logging
import re
import streamlit as st
from snowflake.snowpark.functions import call_udf, concat, lit
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the active Snowflake session
session = get_active_session()
logging.info("Active Snowflake session retrieved.")

# Define database, schema, and search service names
# Update these parameters as needed for your specific Snowflake setup
db_name = '<your_database_name>'
schema_name = '<your_schema_name>'
search_service_name = '<your_search_service_name>'

# Set up the Cortex Search Service Root
root = Root(session)

#------------------------------------------------------------------------------
# UI SETUP - SIDEBAR CONFIGURATION
#------------------------------------------------------------------------------

st.sidebar.title("Settings")

# Model selection dropdown
model_options = [
    'llama3.1-8b',
    'llama3.1-70b',
    'llama3.1-405b',
    'snowflake-arctic',
    'reka-core',
    'reka-flash',
    'mistral-large2',
    'mixtral-8x7b',
    'mistral-7b',
    'jamba-instruct',
    'gemma-7b',
    'deepseek-r1',
    'claude-3-5-sonnet'
]
selected_model = st.sidebar.selectbox(
    'Select Model', 
    model_options, 
    index=model_options.index('llama3.1-70b')
)

# Toggle buttons for Cortex Search and Sources display
col1, col2 = st.sidebar.columns(2)
with col1:
    cortex_search_on = st.toggle('Use Cortex Search', value=True)
with col2:
    # Initialize show_sources in session state if not already set
    if "show_sources" not in st.session_state:
        st.session_state.show_sources = True
    
    # Create the toggle button for sources
    show_sources = st.toggle('Show Sources', value=st.session_state.show_sources)
    
    # Update session state when toggle changes
    if show_sources != st.session_state.show_sources:
        st.session_state.show_sources = show_sources
        # Force a rerun to update the UI when sources are toggled
        st.rerun()

# Context window settings
st.sidebar.markdown("---")
st.sidebar.subheader("Context Window Settings")

# Configure history limit for conversation context
if "history_limit" not in st.session_state:
    st.session_state.history_limit = 10
    
history_limit = st.sidebar.slider(
    'Number of recent messages to include in context',
    min_value=3,
    max_value=20,
    value=st.session_state.history_limit,
    help="Controls how many of the most recent messages are included when sending context to the AI. Lower numbers may improve performance but reduce contextual understanding."
)

# Update session state when slider changes
if history_limit != st.session_state.history_limit:
    st.session_state.history_limit = history_limit

# Document filtering options (only shown when Cortex Search is enabled)
if cortex_search_on:
    # Fetch unique document paths from the database
    query = f"SELECT DISTINCT RELATIVE_PATH FROM {db_name}.{schema_name}.DOCS_CHUNKS_TABLE ORDER BY RELATIVE_PATH"
    try:
        file_df = session.sql(query).collect()
        options = [row['RELATIVE_PATH'] for row in file_df]
        options.insert(0, 'All Documents')
    except Exception as e:
        st.error(f"Error fetching document paths: {str(e)}")
        options = ['All Documents']

    # Document selection and chunk limit controls
    selected_options = st.sidebar.multiselect('Select documents', options, default='All Documents')
    num_chunks = st.sidebar.slider('Number of chunks to use', min_value=1, max_value=10, value=5)

#------------------------------------------------------------------------------
# MAIN UI SETUP
#------------------------------------------------------------------------------

# App title
st.title("❄️ Cortex Search Chat Assistant")

# Initialize chat messages in session state if not already set
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "How can I help you?"}]

#------------------------------------------------------------------------------
# HELPER FUNCTIONS
#------------------------------------------------------------------------------

def highlight_citations(text, show_sources=True):
    """
    Process text to highlight and make citation markers clickable.
    
    Args:
        text (str): The text containing citation markers like [1], [2,3], etc.
        show_sources (bool): Whether to make citations clickable links to sources
    """
    # Split text into paragraphs to preserve formatting
    paragraphs = text.split('\n')
    
    for paragraph in paragraphs:
        if not paragraph.strip():
            # Empty line, just add a line break
            st.write("")
            continue
            
        # Process each paragraph to replace citations with HTML
        processed_paragraph = ""
        parts = re.split(r'(\[\d+(?:,\s*\d+)*\])', paragraph)
        
        for part in parts:
            # If this part is a citation marker
            if re.match(r'\[\d+(?:,\s*\d+)*\]', part):
                # Extract the numbers from the citation
                citation_nums = re.findall(r'\d+', part)
                
                # Add the citation as HTML
                if show_sources:
                    # If sources are shown, make citations clickable
                    citation_html = f'<span style="color: #ff4b4b; font-weight: bold;"><a href="#source_{"_".join(citation_nums)}" style="color: #ff4b4b; text-decoration: none;">{part}</a></span>'
                else:
                    # If sources are hidden, still highlight but don't make clickable
                    citation_html = f'<span style="color: #ff4b4b; font-weight: bold;">{part}</span>'
                processed_paragraph += citation_html
            else:
                # Regular text
                if part:
                    processed_paragraph += part
        
        # Write the entire processed paragraph as a single markdown element
        st.markdown(processed_paragraph, unsafe_allow_html=True)

def display_sources(sources):
    """
    Display source documents in expandable sections with proper formatting.
    
    Args:
        sources (list): List of source documents with their content
    """
    if not show_sources:
        return
        
    st.markdown("---")
    st.markdown("### Sources")
    
    # Display sources using expandable sections
    for i, result in enumerate(sources):
        # Create an anchor for this source
        st.markdown(f'<div id="source_{i+1}"></div>', unsafe_allow_html=True)
        
        # Extract metadata if available
        source_title = f"Source {i+1}"
        if 'metadata' in result and result['metadata'] and 'source' in result['metadata']:
            source_title += f" - {result['metadata']['source']}"
        
        # Create an expander for this source
        with st.expander(source_title):
            # Apply styling for better readability
            st.markdown("""
            <style>
            .source-content {
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 5px;
                line-height: 1.5;
                max-width: 100%;
                white-space: normal;
                overflow-wrap: break-word;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Clean up the text before displaying
            chunk_text = result["chunk"]
            
            # Replace escaped quotes with regular quotes
            chunk_text = chunk_text.replace('\\\"\\\"', '"')
            chunk_text = chunk_text.replace('\\\"', '"')
            
            # Replace \n with actual line breaks
            chunk_text = chunk_text.replace('\\n', '\n')
            
            # Replace multiple consecutive newlines with just two (for paragraph breaks)
            chunk_text = re.sub(r'\n{3,}', '\n\n', chunk_text)
            
            # Handle section headers (like "## BLACK STILL AT LARGE")
            chunk_text = re.sub(r'##\s+(.+)', r'<h3>\1</h3>', chunk_text)
            
            # Convert plain text newlines to HTML breaks for proper rendering
            chunk_text = chunk_text.replace('\n\n', '</p><p>')
            chunk_text = chunk_text.replace('\n', '<br>')
            
            # Wrap in paragraph tags
            chunk_text = f'<p>{chunk_text}</p>'
            
            # Display the cleaned content
            st.markdown(f'<div class="source-content">{chunk_text}</div>', unsafe_allow_html=True)

#------------------------------------------------------------------------------
# DISPLAY CHAT HISTORY
#------------------------------------------------------------------------------

# Display all messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        # If this is an assistant message and we have sources stored for it
        if message["role"] == "assistant" and "source_data" in message:
            # Display the message with citations
            highlight_citations(message["content"], show_sources)
            
            # Display sources if enabled
            if "source_data" in message:
                display_sources(message["source_data"])
        else:
            # Regular message without sources
            st.write(message["content"])

#------------------------------------------------------------------------------
# HANDLE USER INPUT
#------------------------------------------------------------------------------

# Get user input
prompt = st.chat_input("Ask a question...")

# Process the user input
if prompt:
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.write(prompt)
    
    # Build conversation history with limited context window
    recent_messages = st.session_state.messages[:-1]  # Exclude the current prompt
    if len(recent_messages) > st.session_state.history_limit:
        recent_messages = recent_messages[-st.session_state.history_limit:]
    
    # Format conversation history
    conversation_history = ""
    for message in recent_messages:
        role = "User" if message["role"] == "user" else "Assistant"
        conversation_history += f"{role}: {message['content']}\n\n"
    
    # Initialize variables for search results
    context = ""
    error_occurred = False
    
    #--------------------------------------------------------------------------
    # CORTEX SEARCH (if enabled)
    #--------------------------------------------------------------------------
    if cortex_search_on:
        # Determine filter based on selected documents
        filter_dict = None
        if 'All Documents' not in selected_options and selected_options:
            filter_dict = {"@eq": {"relative_path": selected_options[0]}}

        # Query the Cortex Search Service
        try:
            cortex_service = root.databases[db_name].schemas[schema_name].cortex_search_services[search_service_name]
            
            # Execute search with or without filter
            question_response = cortex_service.search(
                prompt, 
                ["chunk"], 
                filter=filter_dict if filter_dict else None,
                limit=num_chunks
            )

            # Build context string from search results
            for i, result in enumerate(question_response.results):
                context += f"Source {i+1}:\n{result['chunk']}\n\n"

        except Exception as e:
            st.error(f"An error occurred while querying Cortex Search: {str(e)}")
            error_occurred = True
    
    #--------------------------------------------------------------------------
    # BUILD SYSTEM MESSAGE
    #--------------------------------------------------------------------------
    if cortex_search_on and not error_occurred:
        # Create source previews for citation guidance
        source_list = ""
        for i, result in enumerate(question_response.results):
            # Get the first 100 characters of each source as a preview
            preview = result['chunk'][:100] + "..." if len(result['chunk']) > 100 else result['chunk']
            source_list += f"Source {i+1}: {preview}\n\n"
        
        # RAG-specific system message
        system_message = f"""You are an AI assistant specifically designed to answer questions based solely on the provided context. Your knowledge is limited to the information below.
        
Context:
{context}

Sources:
{source_list}

These sources are chunks from documents. Sources with the same number prefix come from the same document.

Instructions:
1. Carefully analyze the provided context.
2. Answer questions only based on the above information.
3. If the context lacks sufficient details, state specifically what information is missing. For example: "The provided sources mention X but don't specify Y, which would be needed to fully answer this question."
4. Maintain a professional and concise tone.
5. IMPORTANT: For each claim or piece of information in your response, add a citation marker like [1], [2], etc. that corresponds to the source number in the context. If a claim comes from multiple sources, include all relevant numbers like [1,3].
6. Only cite sources that directly support your statement. Don't cite sources that weren't used.
7. Be precise with your citations - make sure each citation points to a source that actually contains that specific information.
8. Structure your answers in this format when possible:
   - Start with a direct answer to the question
   - Provide supporting details with proper citations
   - If appropriate, include a brief summary at the end
9. When answering complex questions, briefly explain your reasoning, showing how you arrived at the answer based on the provided sources.
10. Be precise in your statements. Don't generalize beyond what the sources explicitly state. Maintain factual accuracy at all costs.
11. If metadata about the source (like document title or category) is available, you can mention it to provide additional context for your answer.
"""
    else:
        # General-purpose system message (when Cortex Search is off)
        system_message = """You are an advanced AI assistant designed to provide exceptional support. You have been trained on a wide range of knowledge and can answer questions across many domains.

Instructions:
1. Answer questions to the best of your abilities using your internal knowledge.
2. Express uncertainty when you don't know something rather than making up information.
3. Maintain a professional and concise tone in your responses.
4. Structure your answers in this format when possible:
   - Start with a direct answer to the question - Format so it is easy to read and understand
   - Provide supporting details and explanations if needed
   - If appropriate, include a brief summary at the end - YOU DO NOT ALWAYS HAVE TO DO THIS
5. When answering complex questions, briefly explain your reasoning to help the user understand your thought process.
6. Be precise in your statements and make clear distinctions between facts, opinions, and speculations.
7. Consider the context of the conversation when formulating your responses.
8. If the user asks for creative content like code, stories, or business ideas, feel free to be imaginative while ensuring practical usefulness.
"""
    
    #--------------------------------------------------------------------------
    # GENERATE AND DISPLAY RESPONSE
    #--------------------------------------------------------------------------
    # Combine system instructions with conversation history and the new prompt
    full_prompt = f"{system_message}\n{conversation_history}\nUser: {prompt}"
    
    # Call the Cortex complete UDF
    try:
        # Generate response using selected model
        response_df = session.create_dataframe([full_prompt]).select(
            call_udf('snowflake.cortex.complete', selected_model, concat(lit(full_prompt)))
        )
        full_response = response_df.collect()[0][0]
        
        # Store the response with source data if available
        response_message = {"role": "assistant", "content": full_response}
        if cortex_search_on and not error_occurred and 'question_response' in locals():
            response_message["source_data"] = question_response.results
        
        # Add response to chat history
        st.session_state.messages.append(response_message)
        
        # Display the response with clickable citation links
        with st.chat_message("assistant"):
            highlight_citations(full_response, show_sources)
            
            # Display sources if enabled
            if cortex_search_on and not error_occurred and 'question_response' in locals():
                display_sources(question_response.results)
    except Exception as e:
        st.error(f"An error occurred while processing the response: {str(e)}")