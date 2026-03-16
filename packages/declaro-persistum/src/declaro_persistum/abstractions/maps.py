"""
Map abstraction using junction tables with key/value columns.

This provides a portable way to store key-value pairs that works
across PostgreSQL and SQLite without relying on JSONB or hstore.
"""

import re
from typing import Any


def parse_map_type(type_str: str) -> tuple[str, str] | None:
    """
    Parse map<key_type, value_type> type declaration.

    Returns tuple of (key_type, value_type) if it's a map type, None otherwise.

    Examples:
        parse_map_type("map<text, text>") -> ("text", "text")
        parse_map_type("map<text, integer>") -> ("text", "integer")
        parse_map_type("map< text , integer >") -> ("text", "integer")
        parse_map_type("text") -> None
    """
    match = re.match(r"^map<\s*([^,]+?)\s*,\s*(.+?)\s*>$", type_str.strip())
    if match:
        return (match.group(1).strip(), match.group(2).strip())
    return None


def generate_junction_table(
    parent_table: str,
    column_name: str,
    key_type: str,
    value_type: str,
    parent_pk: str = "id",
) -> dict[str, Any]:
    """
    Generate junction table schema for map column.

    Args:
        parent_table: Name of the parent table (e.g., "users")
        column_name: Name of the map column (e.g., "metadata")
        key_type: Type of map keys (e.g., "text")
        value_type: Type of map values (e.g., "text")
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
                "key": {
                    "type": key_type,
                    "nullable": False,
                },
                "value": {
                    "type": value_type,
                    "nullable": True,
                },
            },
            "indexes": {
                f"idx_{junction_name}_unique_key": {
                    "columns": [fk_column, "key"],
                    "unique": True,
                },
            },
        }
    }


def map_set_sql(parent_table: str, column_name: str, dialect: str = "postgresql") -> str:
    """
    Generate UPSERT SQL for map entry.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the map column
        dialect: Database dialect ("postgresql" or "sqlite")

    Returns:
        INSERT ... ON CONFLICT UPDATE SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    if dialect in ("sqlite", "turso"):
        # SQLite/Turso use :value placeholder in UPDATE
        return f"""INSERT INTO {junction_name} ({fk_column}, key, value)
VALUES (:parent_id, :key, :value)
ON CONFLICT ({fk_column}, key) DO UPDATE SET value = :value"""
    else:
        # PostgreSQL uses EXCLUDED.value
        return f"""INSERT INTO {junction_name} ({fk_column}, key, value)
VALUES (:parent_id, :key, :value)
ON CONFLICT ({fk_column}, key) DO UPDATE SET value = EXCLUDED.value"""


def map_get_sql(parent_table: str, column_name: str) -> str:
    """
    Generate SELECT SQL for single key lookup.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the map column

    Returns:
        SELECT SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"SELECT value FROM {junction_name} WHERE {fk_column} = :parent_id AND key = :key"


def map_get_all_sql(parent_table: str, column_name: str) -> str:
    """
    Generate SELECT SQL for all map entries.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the map column

    Returns:
        SELECT SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"SELECT key, value FROM {junction_name} WHERE {fk_column} = :parent_id"


def map_delete_sql(parent_table: str, column_name: str) -> str:
    """
    Generate DELETE SQL for single key.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the map column

    Returns:
        DELETE SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"DELETE FROM {junction_name} WHERE {fk_column} = :parent_id AND key = :key"


def map_clear_sql(parent_table: str, column_name: str) -> str:
    """
    Generate DELETE SQL to remove all map entries.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the map column

    Returns:
        DELETE SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"DELETE FROM {junction_name} WHERE {fk_column} = :parent_id"


def map_keys_sql(parent_table: str, column_name: str) -> str:
    """
    Generate SELECT SQL for all keys in map.

    Args:
        parent_table: Name of the parent table
        column_name: Name of the map column

    Returns:
        SELECT SQL statement.
    """
    junction_name = f"{parent_table}_{column_name}"
    fk_column = f"{parent_table[:-1] if parent_table.endswith('s') else parent_table}_id"

    return f"SELECT key FROM {junction_name} WHERE {fk_column} = :parent_id"


def map_hydrate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Reconstruct map from junction table rows.

    Args:
        rows: List of dicts with 'key' and 'value' keys

    Returns:
        Dict mapping keys to values.
    """
    if not rows:
        return {}

    return {r["key"]: r["value"] for r in rows}
