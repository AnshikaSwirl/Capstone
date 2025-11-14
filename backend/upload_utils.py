# backend/upload_utils.py
import os
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import secure_filename  # pip install Werkzeug

from backend.database import get_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Safety / config
ALLOWED_EXT = {".csv", ".xlsx", ".xls"}
MAX_ROWS = int(os.environ.get("UPLOAD_MAX_ROWS", 5_000_000))  # hard cap to avoid huge imports
CHUNK_SIZE = int(os.environ.get("UPLOAD_CHUNK_SIZE", 50_000))  # rows per chunk for to_sql
DEFAULT_SCHEMA = os.environ.get("DB_DEFAULT_SCHEMA", None)  # e.g., "dbo" or None


def _validate_filename(filename: str) -> str:
    """Return a secure filename or raise ValueError."""
    name = secure_filename(filename)
    if not name:
        raise ValueError("Invalid filename")
    return name


def _read_table_from_file(path: str) -> pd.DataFrame:
    p = Path(path)
    ext = p.suffix.lower()
    if ext not in ALLOWED_EXT:
        raise ValueError(f"Unsupported file type: {ext}. Allowed: {sorted(ALLOWED_EXT)}")

    if ext == ".csv":
        # Use low_memory=False to avoid dtype inference issues
        df = pd.read_csv(path, low_memory=False)
    else:
        # For Excel files, read the first sheet
        df = pd.read_excel(path)
    return df


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names, drop duplicates, and fill/convert NA safely.
    This follows the simple rules you had but is more defensive.
    """
    # Normalize column names: strip, lower, replace spaces/hyphens with underscore
    df = df.copy()
    df.columns = [
        str(c).strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns
    ]

    # Drop duplicate rows
    df.drop_duplicates(inplace=True)

    # Replace NaN with None for SQL compatibility
    df = df.where(pd.notnull(df), None)

    return df


def _ensure_table_name_safe(name: str) -> str:
    # basic sanitization — do NOT allow SQL identifiers with spaces or strange chars
    n = str(name).strip().lower()
    if not n:
        raise ValueError("Table name must not be empty")
    # allow only alphanumeric and underscores
    if not all(c.isalnum() or c == "_" for c in n):
        raise ValueError("Table name may only contain letters, numbers and underscores")
    return n


def _to_sql_with_chunks(df: pd.DataFrame, table_name: str, engine, schema: str | None = None):
    """
    Write dataframe to SQL in chunks to avoid memory pressure.
    Uses if_exists='replace' semantics in first chunk and then append.
    """
    kwargs = {"index": False}
    if schema:
        kwargs["schema"] = schema

    # First, drop existing table (replace semantics). Keep in a transaction if supported.
    with engine.begin() as conn:
        try:
            # Remove table if it exists (SQLAlchemy text to avoid reflection issues)
            drop_sql = f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL DROP TABLE {table_name};"
            conn.execute(text(drop_sql))
        except Exception:
            # ignore if drop fails (table may not exist or dialect differences)
            logger.debug("Could not drop prior table (may not exist)")

    # Write in chunks
    total = len(df)
    if total == 0:
        raise ValueError("Uploaded file contains no rows")

    start = 0
    first_chunk = True
    while start < total:
        end = min(start + CHUNK_SIZE, total)
        chunk = df.iloc[start:end]
        try:
            if first_chunk:
                chunk.to_sql(table_name, con=engine, if_exists="replace", **kwargs)
                first_chunk = False
            else:
                chunk.to_sql(table_name, con=engine, if_exists="append", **kwargs)
        except SQLAlchemyError as e:
            logger.exception("Failed to write chunk to SQL: %s", e)
            raise
        start = end


def upload_new_table(file_path: str, table_name: str) -> str:
    """
    High-level helper to load a CSV/XLSX and upload it to the database.
    Returns a human-friendly message on success or raises on error.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # sanitize and validate names
    _validate_filename(path.name)
    table_name_safe = _ensure_table_name_safe(table_name)

    # read file
    df = _read_table_from_file(str(path))

    # enforce row limits
    if len(df) > MAX_ROWS:
        raise ValueError(f"File too large: {len(df)} rows exceeds max allowed {MAX_ROWS}")

    # normalize dataframe
    df = _normalize_dataframe(df)

    # if empty after cleaning, raise
    if df.empty:
        raise ValueError("No data found after cleaning the uploaded file")

    # upload
    engine = get_engine()
    try:
        _to_sql_with_chunks(df, table_name_safe, engine, schema=DEFAULT_SCHEMA)
        return f"Uploaded {len(df)} rows to table '{table_name_safe}' successfully"
    except Exception as e:
        logger.exception("Upload failed: %s", e)
        # Re-raise with a clearer message
        raise RuntimeError(f"Failed to upload table '{table_name_safe}': {e}") from e
