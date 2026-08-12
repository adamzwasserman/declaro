"""
PostgreSQL database inspector implementation.

Uses information_schema and pg_catalog for complete metadata extraction.
"""

from typing import Any

from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import normalize_fk_action as _normalize_fk_action
from declaro_persistum.types import Column, Index, Schema, Table, View




def _normalize_view_query(query: str) -> str:
    """Normalize view query for consistent comparison."""
    return " ".join(query.split())
