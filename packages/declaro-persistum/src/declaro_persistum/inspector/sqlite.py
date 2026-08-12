"""
SQLite database inspector implementation.

Uses PRAGMA statements for metadata extraction.
Shared logic with Turso via inspector.shared module.
"""

import json
from typing import Any

from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import (
    apply_unique_columns,
    assemble_table,
    columns_from_pragma_rows,
    fk_list_from_pragma_rows,
    indexes_from_rows,
    normalize_fk_action,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.types import Column, Index, Schema, Table, View

# Re-export for any external consumers
_normalize_fk_action = normalize_fk_action


