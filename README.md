# Snowflake Cortex Search and Analyst Demo

This repository contains a collection of tools and notebooks demonstrating the power of Snowflake's Cortex Search and Analyst capabilities. The project includes a Streamlit-based search application and two Jupyter notebooks for different aspects of the implementation.

## Project Structure

### 1. `streamlit_search_app.py`
A modern, interactive Streamlit application that provides a chat interface for querying documents using Snowflake's Cortex Search capabilities.

**Key Features:**
- Interactive chat interface with AI-powered responses
- Multiple model selection options (including llama3.1, snowflake-arctic, and others)
- Configurable context window settings
- Document filtering and chunk management
- Source citation and highlighting
- Beautiful UI with expandable source sections

**Usage:**
1. Set up the required database, schema, and search service names
2. Run the Streamlit app using `streamlit run streamlit_search_app.py`

### 2. `Cortex Search Build.ipynb`
A comprehensive notebook that guides you through setting up the Cortex Search infrastructure in Snowflake.

**Key Components:**
- Database and schema creation
- Custom UDF for text chunking
- Stage setup for document storage
- Table creation for storing document chunks
- Document parsing and chunking process
- Integration with Snowflake's Cortex Search service

**Setup Steps:**
1. Create necessary database and schema
2. Set up the text chunking UDF
3. Create a stage for document storage
4. Upload documents to the stage
5. Create and populate the chunks table
6. Configure Cortex Search service

### 3. `Analyst API Endpoint.ipynb`
A demonstration notebook showcasing how to interact with Snowflake's Cortex Analyst API.

**Key Features:**
- API endpoint configuration
- Semantic model integration
- Natural language query processing
- SQL statement extraction and execution
- Response parsing and analysis

**Usage:**
1. Configure your database, schema, and stage names
2. Set up your semantic model file
3. Make API calls with natural language queries
4. Extract and execute SQL statements from responses
5. Analyze the results

### 4. `LLM_PROMPT_ENGINEERING_SF.ipynb`
A comprehensive guide to prompt engineering with Snowflake Cortex, demonstrating best practices for leveraging Snowflake's AI_COMPLETE function and other AI SQL functions.

**Key Topics Covered:**
- **Simple Prompts:** Direct questions using default model parameters
- **LLM as a Judge:** Using models to evaluate other model outputs with structured scoring
- **Chain-of-Thought Reasoning:** Step-by-step analysis using `<thinking>` and `<answer>` tags
- **Structured Output:** JSON schema enforcement for reliable data extraction
- **Model Parameter Tuning:** Controlling temperature, top_p, and max_tokens for different use cases
- **Few-Shot Prompting:** Providing examples to guide model behavior
- **Business Decision Analysis:** Complex reasoning for strategic decisions
- **Role Prompting:** Setting assistant personas and behaviors
- **Classification and Filtering:** Using AI_CLASSIFY, AI_FILTER, and AI_SIMILARITY functions
- **Guardrails and Data Privacy:** Protecting sensitive information in AI applications

**Best Practices Highlighted:**
- Clear and specific instructions with structural markers
- Repeating critical instructions at the end of prompts
- Using tags to separate content, reasoning, and outputs
- Providing diverse examples for pattern learning
- Tuning model parameters based on use case requirements
- Implementing data privacy safeguards

**Usage:**
1. Review the best practices section for effective prompt design
2. Explore different prompting techniques with provided SQL examples
3. Experiment with model parameters for your specific use cases
4. Implement appropriate guardrails for production applications

### 5. `batch_cortex_analyst_tester.py`
A Streamlit application designed to run within Snowflake for batch testing of Cortex Analyst queries. This tool allows you to test multiple natural language questions against your semantic model in one go and analyze the results.

**Key Features:**
- **Batch Processing:** Test multiple natural language questions simultaneously
- **Comprehensive Results:** Captures interpretation, follow-up suggestions, generated SQL, and query execution results
- **Query Execution:** Automatically executes generated SQL with configurable row limits for preview
- **Result Management:** 
  - Download results as CSV
  - Save results directly to Snowflake tables (create new, replace, or append)
  - Clear and re-run tests as needed
- **Metadata Tracking:** Records Query IDs, Request IDs, and timestamps for audit trails
- **Error Handling:** Graceful handling of failed queries with detailed error messages
- **Interactive UI:** Clean interface with progress tracking and expandable options

**Workflow:**
1. Provide the fully-qualified stage path to your semantic model YAML file
2. Enter multiple questions (one per line) in natural language
3. Configure preview row limits and processing options
4. Run batch tests to get comprehensive results including:
   - Analyst's interpretation of each question
   - Generated SQL statements
   - Query execution results (preview)
   - Follow-up question suggestions
   - Query and Request IDs for tracking

**Usage:**
1. Deploy as a Streamlit app in Snowflake
2. Configure your semantic model stage path
3. Input your test questions (one per line)
4. Review results and download or save to Snowflake tables
5. Use for regression testing, model validation, and query optimization