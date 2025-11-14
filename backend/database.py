# backend/database.py
import os
import logging
from urllib.parse import quote_plus
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Optional: load .env during local development
# Install python-dotenv and uncomment the next two lines if you want .env support
# from dotenv import load_dotenv
# load_dotenv()

# Required environment variables (set these in your deployment environment or .env file)
AZ_SQL_SERVER = os.environ.get("AZ_SQL_SERVER")
AZ_SQL_DB = os.environ.get("AZ_SQL_DB")
AZ_SQL_USER = os.environ.get("AZ_SQL_USER")
AZ_SQL_PASSWORD = os.environ.get("AZ_SQL_PASSWORD")
AZ_SQL_DRIVER = os.environ.get("AZ_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

if not all([AZ_SQL_SERVER, AZ_SQL_DB, AZ_SQL_USER, AZ_SQL_PASSWORD]):
    logger.warning(
        "One or more Azure SQL environment variables are missing. "
        "Make sure AZ_SQL_SERVER, AZ_SQL_DB, AZ_SQL_USER, AZ_SQL_PASSWORD are set."
    )

def _build_connection_string():
    # Construct a safe ODBC connection string and URL-encode it for SQLAlchemy
    params = (
        f"Driver={AZ_SQL_DRIVER};"
        f"Server={AZ_SQL_SERVER};"
        f"Database={AZ_SQL_DB};"
        f"Encrypt=yes;"
        # Do not use TrustServerCertificate=yes in production unless you understand the implications
        f"TrustServerCertificate=no;"
    )
    odbc_conn = quote_plus(params)
    user = quote_plus(AZ_SQL_USER or "")
    pwd = quote_plus(AZ_SQL_PASSWORD or "")
    # Use odbc_connect param to avoid embedding server/db in the URL path
    return f"mssql+pyodbc://{user}:{pwd}@/?odbc_connect={odbc_conn}"

# Engine singleton
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        conn_str = _build_connection_string()
        # Tune pool settings for production load
        _engine = create_engine(
            conn_str,
            pool_size=int(os.environ.get("DB_POOL_SIZE", 10)),
            max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", 20)),
            pool_pre_ping=True,
            pool_timeout=int(os.environ.get("DB_POOL_TIMEOUT", 30)),
            connect_args={"timeout": int(os.environ.get("DB_CONNECT_TIMEOUT", 30))},
        )
        logger.info("SQLAlchemy engine created")
    return _engine

# Retry transient DB connection errors
@retry(
    retry=retry_if_exception_type(OperationalError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _connect_and_execute(sql_text, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql_text), params or {})
        return result

def get_table_schema(table_name: str):
    """
    Returns a dict of column_name: data_type for the requested table.
    Validates that the table exists and raises ValueError if not present.
    """
    engine = get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if table_name not in tables:
        raise ValueError(f"Table not found: {table_name}")
    columns = inspector.get_columns(table_name)
    return {col["name"]: str(col["type"]) for col in columns}

def get_all_table_names():
    """
    Returns a list of all base table names in the database.
    """
    sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
    rows = _connect_and_execute(sql)
    return [row[0] for row in rows]

def safe_select_all(table_name: str, limit: int = 1000):
    """
    Return rows for a table with a safety limit to avoid huge payloads.
    Use parameterized queries for any values; table_name is validated.
    """
    # validate table name exists
    engine = get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if table_name not in tables:
        raise ValueError(f"Table not found: {table_name}")

    # Build safe SQL (table name validated above)
    sql = f"SELECT TOP ({int(limit)}) * FROM {table_name};"
    rows = _connect_and_execute(sql)
    return rows.fetchall()

# Example helper to execute arbitrary read-only SQL (parameterized)
def execute_read_query(sql_text: str, params: dict = None):
    """
    Execute a read query. Use only for SELECT queries and when sql_text is trusted
    or fully parameterized. This does NOT protect table-name interpolation.
    """
    sql_text = sql_text.strip()
    if not sql_text.lower().startswith("select"):
        raise ValueError("execute_read_query only supports SELECT statements")
    rows = _connect_and_execute(sql_text, params or {})
    return rows.fetchall()
