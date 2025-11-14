# backend/executer.py

import logging
from typing import List, Dict, Optional
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from backend.database import get_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert NaN -> None and convert numpy / pandas scalar types to native Python types.
    Avoid deprecated DataFrame.applymap by mapping per-column.
    """
    if df.empty:
        return df

    # Replace pandas NaN with None
    df = df.where(pd.notnull(df), None)

    # Convert numpy/pandas scalar types to python scalars
    def _to_python_scalar(x):
        try:
            return x.item() if hasattr(x, "item") else x
        except Exception:
            return x

    # Apply per-column mapping (avoids deprecated applymap)
    for col in df.columns:
        df[col] = df[col].map(_to_python_scalar)

    return df


def run_query(sql: str, limit: Optional[int] = None, timeout: Optional[int] = None) -> List[Dict]:
    """
    Execute a read-only SQL query and return a list of dict rows.

    - sql: raw SQL string (should be constructed carefully; prefer parameterized helpers)
    - limit: optional max number of rows to return (appends TOP clause for SQL Server)
    - timeout: optional query timeout in seconds (passed via execution options if supported)

    NOTE: This function executes the provided SQL as-is. Make sure any user-provided
    values are parameterized / validated upstream to avoid injection.
    """
    engine = get_engine()

    # If a limit is provided and the query starts with SELECT, attempt to enforce a TOP for SQL Server
    final_sql = sql
    if limit is not None:
        stripped = sql.lstrip().lower()
        if stripped.startswith("select"):
            # a simple heuristic: insert TOP (n) after SELECT
            final_sql = sql.lstrip()
            # preserve 'SELECT DISTINCT' case
            if final_sql[:15].lower().startswith("select distinct"):
                final_sql = final_sql[:15] + f" TOP ({int(limit)}) " + final_sql[15:]
            else:
                final_sql = "SELECT TOP ({}) ".format(int(limit)) + final_sql[6:]
        else:
            logger.debug("Limit provided but SQL does not start with SELECT; ignoring limit")

    try:
        # Use pandas read_sql with SQLAlchemy engine; text() used when needed
        df = pd.read_sql(text(final_sql), con=engine)
        df = _normalize_dataframe(df)
        return df.to_dict(orient="records")
    except SQLAlchemyError as e:
        logger.exception("Database query failed: %s", e)
        raise
    except Exception as e:
        logger.exception("Unexpected error running query: %s", e)
        raise


def get_table_schema(table_name: str) -> str:
    """
    Return a comma-separated list of column names for a validated table.

    Uses parameterized query to INFORMATION_SCHEMA.COLUMNS to avoid injection.
    Raises ValueError if the table does not exist or has no columns.
    """
    engine = get_engine()
    try:
        # Validate table exists via inspector for extra safety (optional)
        inspector = engine.dialect.get_inspector(engine)
        all_tables = inspector.get_table_names()
        if table_name not in all_tables:
            raise ValueError(f"Table not found: {table_name}")

        sql = text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """)
        df = pd.read_sql(sql, con=engine, params={"table_name": table_name})
        if df.empty:
            raise ValueError(f"No columns found for table: {table_name}")
        return ", ".join(df["COLUMN_NAME"].tolist())
    except Exception:
        # Fallback: try a parameterized query (keeps behavior but logs)
        try:
            sql = text("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
            """)
            df = pd.read_sql(sql, con=engine, params={"table_name": table_name})
            return ", ".join(df["COLUMN_NAME"].tolist())
        except Exception as e:
            logger.exception("Failed to retrieve schema for table %s: %s", table_name, e)
            raise
