# backend/graph_agent.py

import os
import time
import logging
from typing import TypedDict, Optional

from langgraph.graph import StateGraph, START, END
from backend.sql_generator import generate_sql_gemini
from backend.database import get_table_schema, get_all_table_names
from backend.executer import run_query

# Azure OpenAI client import (keep existing library you use)
from openai import AzureOpenAI
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Read Azure OpenAI settings from environment (do NOT hardcode credentials)
AZURE_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY = os.environ.get("AZURE_OPENAI_API_KEY")
AZURE_API_VERSION = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
AZURE_DEPLOYMENT = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-5-mini-2")

if not AZURE_ENDPOINT or not AZURE_API_KEY:
    logger.warning("Azure OpenAI endpoint or API key not set in environment variables.")

client = AzureOpenAI(
    azure_endpoint=AZURE_ENDPOINT,
    api_key=AZURE_API_KEY,
    api_version=AZURE_API_VERSION,
)

class AgentState(TypedDict, total=False):
    query: str
    table_name: Optional[str]
    sql: Optional[str]
    result: Optional[str]
    answer: Optional[str]
    memory: Optional[str]
    filters: Optional[str]
    last_action: Optional[str]
    last_group: Optional[str]


def identify_table_step(state: AgentState) -> AgentState:
    logger.info("Identifying best table for query: %s", state.get("query"))
    # Reuse previously selected table unless the user explicitly asks for a different table
    if state.get("table_name"):
        logger.info("Reusing table: %s", state["table_name"])
        return state

    try:
        all_tables = get_all_table_names()
    except Exception as e:
        logger.exception("Failed to list tables: %s", e)
        # leave table_name unset so generator will fail gracefully
        return state

    # Guard: if there are no tables, return
    if not all_tables:
        logger.warning("No tables found in database")
        return state

    # Build compact schema text for the prompt
    table_schemas = {}
    for t in all_tables:
        try:
            table_schemas[t] = get_table_schema(t)
        except Exception:
            table_schemas[t] = "(schema unavailable)"

    schema_text = "\n\n".join([f"Table: {t}\nColumns: {table_schemas[t]}" for t in table_schemas])

    prompt = f"""
You are a data expert.
Given the user's query and available tables, choose the ONE table that best matches the query.

User Query: {state.get('query')}
Available Tables and Schemas:
{schema_text}

Return only the table name (no explanation). If no table fits, return 'none'.
"""

    try:
        response = client.chat.completions.create(
            model=AZURE_DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are an intelligent SQL assistant."},
                {"role": "user", "content": prompt},
            ],
        )
        chosen_table = response.choices[0].message.content.strip()
        # Validate the chosen table
        if chosen_table.lower() == "none" or chosen_table not in all_tables:
            logger.warning("Model chose invalid or no table: %s", chosen_table)
            # Do not set table_name if invalid
            return state
        state["table_name"] = chosen_table
        logger.info("Selected table: %s", chosen_table)
    except Exception as e:
        logger.exception("Table identification failed: %s", e)

    return state


def generate_sql_step(state: AgentState) -> AgentState:
    table = state.get("table_name")
    logger.info("Generating SQL for table: %s", table)

    if not table:
        logger.warning("No table selected; skipping SQL generation")
        return state

    # Ensure table exists
    try:
        schema = get_table_schema(table)
    except Exception as e:
        logger.exception("Failed to fetch schema for %s: %s", table, e)
        return state

    query_text = state.get("query", "").lower()

    # Gender-swap followup pattern
    try:
        if "male" in query_text and state.get("filters"):
            new_filters = state["filters"].replace("Female", "Male").replace("'female'", "'male'")
            sql = f"SELECT * FROM {table} WHERE {new_filters};"
            state["filters"] = new_filters
            state["last_action"] = "select_records"

        elif "break down" in query_text and state.get("filters"):
            if "userage" in query_text:
                group_col = "userage"
            elif "usergender" in query_text:
                group_col = "usergender"
            else:
                group_col = state.get("last_group", "usergender")

            sql = f"""
SELECT {group_col},
       COUNT(*) AS review_count,
       AVG(reviewrating) AS avg_rating,
       MIN(reviewrating) AS min_rating,
       MAX(reviewrating) AS max_rating
FROM {table}
WHERE {state['filters']}
GROUP BY {group_col}
ORDER BY review_count DESC;
"""
            state["last_action"] = "breakdown"
            state["last_group"] = group_col

        else:
            enriched_query = f"Conversation so far:\n{state.get('memory','')}\n\nUser Query: {state.get('query')}\n"
            # generate_sql_gemini should be safe and return a full SQL string
            sql = generate_sql_gemini(enriched_query, schema, table) or ""
            # If filters exist and SQL lacks WHERE, append them (safe only if filters were generated internally)
            if state.get("filters") and "WHERE" not in sql.upper():
                sql = sql.strip().rstrip(";") + f"\nWHERE {state['filters']};"

        # Final sanity: sql must be non-empty and start with SELECT
        if not sql or not sql.strip().lower().startswith("select"):
            logger.warning("Generated SQL is empty or not a SELECT: %s", sql)
            state["sql"] = sql
            return state

        state["sql"] = sql
        logger.info("SQL Generated successfully")
    except Exception as e:
        logger.exception("Error while generating SQL: %s", e)

    return state


def execute_sql_step(state: AgentState) -> AgentState:
    sql = state.get("sql")
    logger.info("Executing SQL query...")
    if not sql:
        logger.warning("No SQL to execute")
        return state

    try:
        t0 = time.time()
        # run_query should return list of dict rows
        rows = run_query(sql)
        duration = time.time() - t0
        logger.info("Query executed in %.2fs; rows=%d", duration, len(rows) if isinstance(rows, list) else 0)
        state["result"] = str(rows)
        # Capture filters safely (only if SQL contained a WHERE)
        if "WHERE" in sql.upper():
            where_clause = sql.split("WHERE", 1)[1]
            where_clause = where_clause.split("GROUP BY")[0].split("ORDER BY")[0].strip()
            state["filters"] = where_clause
            logger.info("Captured filters: %s", state["filters"])
    except SQLAlchemyError as e:
        logger.exception("Database error during query execution: %s", e)
        state["result"] = f"Database error: {e}"
    except Exception as e:
        logger.exception("Unexpected error during query execution: %s", e)
        state["result"] = f"Execution error: {e}"

    return state


def summarize_step(state: AgentState) -> AgentState:
    logger.info("Summarizing results into natural language...")

    summary_prompt = f"""
Conversation Memory:
{state.get('memory', 'None')}

User Query: {state.get('query')}
SQL Result: {state.get('result')}
Table: {state.get('table_name')}

Write a clear, detailed answer in full sentences.
- Only describe the SQL result from table {state.get('table_name')}.
- Do not invent or switch to other datasets.
- Always explain the result in context.
- Include percentages or comparisons if relevant.
- Do not suggest next steps or ask questions.
"""

    try:
        response = client.chat.completions.create(
            model=AZURE_DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a helpful data analyst who speaks in plain English."},
                {"role": "user", "content": summary_prompt},
            ],
            # you can add request-specific timeouts/config here if supported
        )
        answer = response.choices[0].message.content.strip()
        state["answer"] = answer
        previous_memory = state.get("memory", "")
        state["memory"] = f"{previous_memory}\nUser: {state.get('query')}\nAI: {answer}\n"
        logger.info("Summarization complete")
    except Exception as e:
        logger.exception("Summarization failed: %s", e)
        state["answer"] = f"Summarization error: {e}"

    return state


# Build LangGraph Flow
agent_graph = StateGraph(AgentState)
agent_graph.add_node("identify_table", identify_table_step)
agent_graph.add_node("generate_sql", generate_sql_step)
agent_graph.add_node("execute_sql", execute_sql_step)
agent_graph.add_node("summarize", summarize_step)
agent_graph.add_edge(START, "identify_table")
agent_graph.add_edge("identify_table", "generate_sql")
agent_graph.add_edge("generate_sql", "execute_sql")
agent_graph.add_edge("execute_sql", "summarize")
agent_graph.add_edge("summarize", END)

graph_agent = agent_graph.compile()
