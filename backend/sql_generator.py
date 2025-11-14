# backend/sql_generator.py
import os
import logging
import time
from typing import Optional

from openai import AzureOpenAI

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Read Azure OpenAI config from environment (do NOT hardcode)
AZURE_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY = os.environ.get("AZURE_OPENAI_API_KEY")
AZURE_DEPLOYMENT = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-5-mini-2")
AZURE_API_VERSION = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

if not AZURE_ENDPOINT or not AZURE_API_KEY:
    logger.warning("Azure OpenAI endpoint or API key not found in environment variables.")

_client: Optional[AzureOpenAI] = None


def _get_client() -> AzureOpenAI:
    global _client
    if _client is None:
        _client = AzureOpenAI(
            azure_endpoint=AZURE_ENDPOINT,
            api_key=AZURE_API_KEY,
            api_version=AZURE_API_VERSION,
        )
    return _client


def _clean_model_sql(text: str) -> str:
    """
    Remove markdown fences and trailing whitespace from the model output.
    Collapse whitespace into single spaces.
    """
    if not text:
        return text
    cleaned = text.replace("```sql", "").replace("```", "").strip()
    cleaned = " ".join(cleaned.split())
    return cleaned


def _validate_sql(sql: str, table_name: str) -> bool:
    """
    Basic validation: SQL must start with SELECT and contain the table_name token.
    This is a lightweight guard — upstream should ensure table_name and columns are valid.
    """
    if not sql:
        return False
    lower = sql.strip().lower()
    if not lower.startswith("select"):
        logger.warning("Generated SQL does not start with SELECT: %s", sql)
        return False
    if table_name.lower() not in lower:
        logger.warning("Generated SQL does not reference table %s: %s", table_name, sql)
        return False
    return True


def generate_sql_gemini(user_query: str, schema: str, table_name: str, max_retries: int = 2) -> str:
    """
    Generate SQL using Azure OpenAI. Returns validated SQL string (single-line).
    Raises RuntimeError on failure or validation errors.
    """
    client = _get_client()

    prompt = f"""
You are an SQL expert. Convert the user request into a valid SQL query ONLY.

TABLE NAME: {table_name}

COLUMNS (use exactly these, comma-separated):
{schema}

RULES:
- Use only the table `{table_name}`.
- Do not guess or rename columns; use exactly the provided column names.
- Return ONLY the SQL query. No explanation, no commentary, no code fences.
- Ensure the query is a SELECT statement and references the table name.

User Query: {user_query}
"""

    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": "You are a strict SQL generator."},
                    {"role": "user", "content": prompt},
                ],
            )
            raw_sql = response.choices[0].message.content.strip()
            sql = _clean_model_sql(raw_sql)

            if not _validate_sql(sql, table_name):
                raise RuntimeError(f"Validation failed for generated SQL: {sql}")

            return sql

        except Exception as e:
            last_exception = e
            logger.exception("OpenAI API or generation error (attempt %d/%d): %s", attempt + 1, max_retries + 1, e)
            # backoff before retrying
            time.sleep(1 + attempt * 2)
            continue

    raise RuntimeError(f"Failed to generate SQL after {max_retries + 1} attempts") from last_exception
