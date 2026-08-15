"""
Array abstraction using junction tables with position column.

This provides a portable way to store ordered collections that works
across PostgreSQL and SQLite without relying on native array types.
"""

import re
from collections.abc import Callable
from typing import Any

from declaro_persistum.types import Dialect


def parse_array_type(type_str: str) -> str | None:
    """
    Parse array<element_type> type declaration.

    Returns the element type if it's an array type, None otherwise.

    Examples:
        parse_array_type("array<text>") -> "text"
        parse_array_type("array<integer>") -> "integer"
        parse_array_type("array<array<text>>") -> "array<text>"
        parse_array_type("text") -> None
    """
    match = re.match(r"^array<(.+)>$", type_str.strip())
    if match:
        return match.group(1).strip()
    return None


def generate_junction_table(
    parent_table: str,
    column_name: str,
    element_type: str,
    parent_pk: str = "id",
) -> dict[str, Any]:
    """
    Generate junction table schema for array column.

    Args:
        parent_table: Name of the parent table (e.g., "users")
        column_name: Name of the array column (e.g., "tags")
        element_type: Type of array elements (e.g., "text")
        parent_pk: Primary key column of parent table (default: "id")

    Returns:
        Schema dict with junction table definition.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return {
        junction_name: {
            "columns": {
                "id": {
                    "type": "uuid",
                    "primary_key": True,
                    "default": "gen_random_uuid()",
                },
                fk_column: {
                    "type": "uuid",
                    "nullable": False,
                    "references": f"{parent_table}.{parent_pk}",
                    "on_delete": "cascade",
                },
                "value": {
                    "type": element_type,
                    "nullable": False,
                },
                "position": {
                    "type": "integer",
                    "nullable": False,
                },
            },
            "indexes": {
                f"idx_{junction_name}_position": {
                    "columns": [fk_column, "position"],
                },
            },
        }
    }


def array_insert_sql(parent_table: str, column_name: str) -> str:
    """
    Generate INSERT SQL for array element at specific position.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the array column

    Returns:
        INSERT SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1]}_id" if parent_table.endswith("s") else f"{parent_table}_id"

    return f"INSERT INTO {junction_name} ({fk_column}, value, position) VALUES (:parent_id, :value, :position)"


def array_append_sql(parent_table: str, column_name: str) -> str:
    """
    Generate INSERT SQL to append element at end of array.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the array column

    Returns:
        INSERT SQL statement that calculates next position.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"""INSERT INTO {junction_name} ({fk_column}, value, position)
VALUES (:parent_id, :value, (SELECT COALESCE(MAX(position), -1) + 1 FROM {junction_name} WHERE {fk_column} = :parent_id))"""


def array_get_sql(parent_table: str, column_name: str) -> str:
    """
    Generate SELECT SQL to get all array elements in order.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the array column

    Returns:
        SELECT SQL statement ordered by position.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"SELECT value, position FROM {junction_name} WHERE {fk_column} = :parent_id ORDER BY position"


def array_delete_sql(parent_table: str, column_name: str) -> str:
    """
    Generate DELETE SQL to remove element at specific position.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the array column

    Returns:
        DELETE SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"DELETE FROM {junction_name} WHERE {fk_column} = :parent_id AND position = :position"


def array_clear_sql(parent_table: str, column_name: str) -> str:
    """
    Generate DELETE SQL to remove all elements from array.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the array column

    Returns:
        DELETE SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"DELETE FROM {junction_name} WHERE {fk_column} = :parent_id"


def array_hydrate(rows: list[dict[str, Any]]) -> list[Any]:
    """
    Reconstruct array from junction table rows.

    Args:
        rows: List of dicts with 'value' and 'position' keys

    Returns:
        List of values sorted by position.
    """
    if not rows:
        return []

    # Sort by position and extract values
    sorted_rows = sorted(rows, key=lambda r: r["position"])
    return [r["value"] for r in sorted_rows]


def array_move_sql(parent_table: str, column_name: str) -> str:
    """
    Generate UPDATE SQL to move element to new position.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the array column

    Returns:
        UPDATE SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"UPDATE {junction_name} SET position = :new_position WHERE {fk_column} = :parent_id AND position = :old_position"


def _reindex_dbapi(junction_name: str, fk_column: str) -> str:
    """A correlated count, which every SQLite-family engine can run."""
    return f"""UPDATE {junction_name}
SET position = (
    SELECT COUNT(*) - 1
    FROM {junction_name} AS t2
    WHERE t2.{fk_column} = {junction_name}.{fk_column}
    AND t2.position <= {junction_name}.position
)
WHERE {fk_column} = :parent_id"""


def _reindex_postgresql(junction_name: str, fk_column: str) -> str:
    """UPDATE ... FROM with a window function, which PostgreSQL has."""
    return f"""UPDATE {junction_name} AS t
SET position = s.new_position
FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY {fk_column} ORDER BY position) - 1 AS new_position
    FROM {junction_name}
    WHERE {fk_column} = :parent_id
) AS s
WHERE t.id = s.id"""


REINDEX_SQL: dict[str, Callable[[str, str], str]] = {
    "postgresql": _reindex_postgresql,
    "sqlite": _reindex_dbapi,
    "turso": _reindex_dbapi,
}


def array_reindex_sql(parent_table: str, column_name: str, dialect: Dialect) -> str:
    """Generate SQL to normalize positions to 0, 1, 2, ..., closing gaps.

    THE DIALECT IS LOOKED UP, NOT BRANCHED ON. This was
    `if dialect in ("sqlite", "turso"): ... else: <postgresql>`, and that
    `else` was not a PostgreSQL branch -- it was a catch-all. Measured before
    the change: "mysql", "libsql" and "" all returned PostgreSQL SQL. A typo
    produced syntax for an engine the caller was not on, and the only warning
    was whatever the database said when it rejected it.

    `dialect` is also required now. It carried `= "postgresql"`, so omitting
    it could not be told from choosing it (Rule 14), and which engine this SQL
    is for is never something a caller can be assumed to have meant.
    """
    generate = REINDEX_SQL.get(dialect)
    if generate is None:
        raise ValueError(
            f"cannot generate reindex SQL for dialect {dialect!r}. "
            f"Supported: {sorted(REINDEX_SQL)}."
        )
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"
    return generate(junction_name, fk_column)
