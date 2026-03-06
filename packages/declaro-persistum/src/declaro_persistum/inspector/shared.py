"""
Shared pure functions for SQLite-compatible inspectors (SQLite, Turso/libSQL).

All functions are pure: no I/O, no side effects, no state.
Both SQLiteInspector and TursoInspector delegate shared logic here.
"""

import re
from typing import Any, Literal

from declaro_persistum.types import Column, Index, Table, View

# Type for FK actions
FKAction = Literal["cascade", "set null", "restrict", "no action"]


def normalize_fk_action(action: str | None) -> FKAction | None:
    """Normalize FK action string to proper Literal type."""
    if action is None or action == "NO ACTION":
        return None
    normalized = action.lower().replace(" ", "_")
    action_map = {
        "cascade": "cascade",
        "set_null": "set null",
        "restrict": "restrict",
        "no_action": "no action",
    }
    return action_map.get(normalized)  # type: ignore[return-value]


def normalize_sqlite_type(col_type: str) -> str:
    """Normalize SQLite type to canonical form."""
    if not col_type:
        return "blob"

    col_type = col_type.lower().strip()

    if "int" in col_type:
        return "integer"
    elif "char" in col_type or "clob" in col_type or "text" in col_type:
        return "text"
    elif "blob" in col_type or col_type == "":
        return "blob"
    elif "real" in col_type or "floa" in col_type or "doub" in col_type:
        return "real"
    else:
        return "numeric"


def extract_view_query(create_statement: str) -> str:
    """Extract SELECT query from CREATE VIEW statement."""
    match = re.search(r"\bAS\s+(.+)$", create_statement, re.IGNORECASE | re.DOTALL)
    if match:
        return " ".join(match.group(1).split())
    return ""


def columns_from_pragma_rows(
    rows: list[tuple],
) -> dict[str, Column]:
    """Convert PRAGMA table_info rows to Column dict (pure)."""
    columns: dict[str, Column] = {}

    for row in rows:
        col_name = row[1]
        col_type = row[2]
        not_null = bool(row[3])
        default = row[4]
        is_pk = bool(row[5])

        col: Column = {"type": normalize_sqlite_type(col_type)}

        if not_null:
            col["nullable"] = False

        if default is not None:
            col["default"] = default

        if is_pk:
            col["primary_key"] = True

        columns[col_name] = col

    return columns


def apply_unique_columns(
    columns: dict[str, Column], unique_cols: set[str]
) -> None:
    """Mark unique columns (mutates columns in place)."""
    for col_name in unique_cols:
        if col_name in columns and not columns[col_name].get("primary_key"):
            columns[col_name]["unique"] = True


def unique_cols_from_index_rows(
    index_rows: list[tuple],
    index_info_rows: dict[str, list[tuple]],
) -> set[str]:
    """Extract single-column unique constraint columns from index data (pure)."""
    unique_cols: set[str] = set()

    for idx_row in index_rows:
        idx_name = idx_row[1]
        is_unique = bool(idx_row[2])
        origin = idx_row[3]

        if is_unique and origin != "pk":
            idx_cols = index_info_rows.get(idx_name, [])
            if len(idx_cols) == 1:
                unique_cols.add(idx_cols[0][2])

    return unique_cols


def indexes_from_rows(
    index_rows: list[tuple],
    index_info_rows: dict[str, list[tuple]],
    index_sql: dict[str, str | None],
) -> dict[str, Index]:
    """Build Index dict from index list/info/sql data (pure)."""
    indexes: dict[str, Index] = {}

    for idx_row in index_rows:
        idx_name = idx_row[1]
        is_unique = bool(idx_row[2])
        origin = idx_row[3]

        if origin in ("pk", "u"):
            continue

        idx_cols = index_info_rows.get(idx_name, [])
        columns = [row[2] for row in idx_cols]

        index: Index = {"columns": columns}

        if is_unique:
            index["unique"] = True

        sql = index_sql.get(idx_name)
        if sql and " WHERE " in sql.upper():
            where_idx = sql.upper().index(" WHERE ")
            index["where"] = sql[where_idx + 7:].strip()

        indexes[idx_name] = index

    return indexes


def fk_list_from_pragma_rows(rows: list[tuple]) -> list[dict[str, str]]:
    """Convert PRAGMA foreign_key_list rows to FK dicts (pure)."""
    return [
        {
            "from": row[3],
            "table": row[2],
            "to": row[4],
            "on_update": row[5],
            "on_delete": row[6],
        }
        for row in rows
    ]


def assemble_table(
    columns: dict[str, Column],
    indexes: dict[str, Index],
    foreign_keys: list[dict[str, str]],
) -> Table:
    """Merge FK info into columns and build Table dict (pure)."""
    for fk in foreign_keys:
        col_name = fk["from"]
        if col_name in columns:
            columns[col_name]["references"] = f"{fk['table']}.{fk['to']}"
            on_delete = normalize_fk_action(fk.get("on_delete"))
            if on_delete:
                columns[col_name]["on_delete"] = on_delete
            on_update = normalize_fk_action(fk.get("on_update"))
            if on_update:
                columns[col_name]["on_update"] = on_update

    table: Table = {"columns": columns}

    pk_columns = [name for name, col in columns.items() if col.get("primary_key")]
    if len(pk_columns) > 1:
        table["primary_key"] = pk_columns
        for col_name in pk_columns:
            del columns[col_name]["primary_key"]

    if indexes:
        table["indexes"] = indexes

    return table


def views_from_rows(rows: list[tuple]) -> dict[str, View]:
    """Convert sqlite_master view rows to View dict (pure)."""
    views: dict[str, View] = {}
    for name, sql in rows:
        query = extract_view_query(sql)
        views[name] = {
            "name": name,
            "query": query,
            "materialized": False,
        }
    return views
