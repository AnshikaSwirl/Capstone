# backend/__init__.py
"""Backend package for the Conversational SQL Agent."""

__all__ = [
    "get_engine",
    "run_query",
    "get_table_schema",
    "upload_new_table",
    "graph_agent",
]

# package metadata
__version__ = "0.1.0"

# lightweight re-exports for convenience (no heavy initialization)
from .database import get_engine
from .executer import run_query, get_table_schema
from .upload_utils import upload_new_table
from .agent import graph_agent
