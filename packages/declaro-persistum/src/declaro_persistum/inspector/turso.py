"""
Turso (libSQL) database inspector implementation.

Turso is SQLite-compatible, so this shares logic with SQLite via
inspector.shared module. Uses pragma_compat abstraction for PRAGMA
calls that may not be natively supported by Turso Database (Rust).
"""

from typing import Any

from declaro_persistum.abstractions.pragma_compat import (
    _maybe_await,
    pragma_foreign_key_list,
    pragma_index_info,
    pragma_index_list,
    pragma_table_info,
)
from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import (
    apply_unique_columns,
    assemble_table,
    columns_from_pragma_rows,
    fk_list_from_pragma_rows,
    indexes_from_rows,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.types import Column, Index, Schema, Table, View


