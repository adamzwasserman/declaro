"""
Materialized view abstraction using tables for SQLite/Turso/LibSQL.

This provides emulation of PostgreSQL materialized views on SQLite-family
databases using regular tables with metadata tracking.

Pattern follows arrays.py and maps.py abstractions.
"""

import json
from typing import Any, Literal

# Type for refresh strategies
RefreshStrategy = Literal["manual", "trigger", "hybrid"]

# Metadata table name (prefixed to avoid collision)
MATVIEW_METADATA_TABLE = "_dp_materialized_views"


def _escape_sql_string(s: str) -> str:
    """Escape single quotes in SQL string."""
    return s.replace("'", "''")


def generate_metadata_table_schema() -> dict[str, Any]:
    """
    Generate schema for the materialized views metadata table.

    Returns:
        Table schema dict for _dp_materialized_views
    """
    return {
        MATVIEW_METADATA_TABLE: {
            "columns": {
                "name": {"type": "text", "primary_key": True},
                "query": {"type": "text", "nullable": False},
                "refresh_strategy": {
                    "type": "text",
                    "nullable": False,
                    "default": "'manual'",
                },
                "depends_on": {"type": "text"},  # JSON array
                "last_refreshed_at": {"type": "text"},
                "created_at": {
                    "type": "text",
                    "nullable": False,
                    "default": "(datetime('now'))",
                },
            }
        }
    }


def generate_matview_table_name(view_name: str) -> str:
    """
    Generate the backing table name for an emulated matview.

    We use the same name as the view - the metadata table tracks
    which tables are matviews vs regular tables.
    """
    return view_name


def create_matview_sql(
    name: str,
    query: str,
    refresh_strategy: RefreshStrategy = "manual",
    depends_on: list[str] | None = None,
) -> list[str]:
    """
    Generate SQL statements to create an emulated materialized view.

    Args:
        name: View/table name
        query: SELECT query for the view
        refresh_strategy: How refresh should be handled
        depends_on: List of source table names

    Returns:
        List of SQL statements to execute
    """
    statements: list[str] = []

    # 1. Ensure metadata table exists
    statements.append(
        f"""CREATE TABLE IF NOT EXISTS {MATVIEW_METADATA_TABLE} (
    name TEXT PRIMARY KEY,
    query TEXT NOT NULL,
    refresh_strategy TEXT NOT NULL DEFAULT 'manual',
    depends_on TEXT,
    last_refreshed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)"""
    )

    # 2. Create the backing table from query
    statements.append(f'CREATE TABLE "{name}" AS {query}')

    # 3. Register in metadata
    depends_json = "NULL"
    if depends_on:
        depends_json = f"'{json.dumps(depends_on)}'"

    escaped_query = _escape_sql_string(query)
    statements.append(
        f"""INSERT INTO {MATVIEW_METADATA_TABLE} (name, query, refresh_strategy, depends_on)
VALUES ('{name}', '{escaped_query}', '{refresh_strategy}', {depends_json})"""
    )

    return statements


def drop_matview_sql(name: str) -> list[str]:
    """
    Generate SQL statements to drop an emulated materialized view.

    Args:
        name: View/table name

    Returns:
        List of SQL statements to execute
    """
    return [
        f'DROP TABLE IF EXISTS "{name}"',
        f"DELETE FROM {MATVIEW_METADATA_TABLE} WHERE name = '{name}'",
    ]


def refresh_matview_sql(
    name: str,
    query: str,
    atomic: bool = True,
) -> list[str]:
    """
    Generate SQL statements to refresh an emulated materialized view.

    Args:
        name: View/table name
        query: The SELECT query to re-execute
        atomic: If True, use DELETE+INSERT; if False, recreate table

    Returns:
        List of SQL statements to execute
    """
    if atomic:
        # Atomic refresh: delete all rows, insert new ones
        return [
            f'DELETE FROM "{name}"',
            f'INSERT INTO "{name}" {query}',
            f"UPDATE {MATVIEW_METADATA_TABLE} SET last_refreshed_at = datetime('now') WHERE name = '{name}'",
        ]
    else:
        # Non-atomic: drop and recreate (loses indexes)
        return [
            f'DROP TABLE IF EXISTS "{name}"',
            f'CREATE TABLE "{name}" AS {query}',
            f"UPDATE {MATVIEW_METADATA_TABLE} SET last_refreshed_at = datetime('now') WHERE name = '{name}'",
        ]


def generate_refresh_trigger_sql(
    matview_name: str,
    source_table: str,
    query: str,
    events: list[str] | None = None,
) -> list[str]:
    """
    Generate trigger SQL for auto-refresh on source table changes.

    Args:
        matview_name: The materialized view to refresh
        source_table: Table to watch for changes
        query: The matview query
        events: Events to trigger on (default: INSERT, UPDATE, DELETE)

    Returns:
        List of SQL statements to create triggers
    """
    events = events or ["INSERT", "UPDATE", "DELETE"]
    statements: list[str] = []

    for event in events:
        trigger_name = (
            f"_dp_refresh_{matview_name}_on_{source_table}_{event.lower()}"
        )

        statements.append(
            f"""CREATE TRIGGER IF NOT EXISTS {trigger_name}
AFTER {event} ON "{source_table}"
BEGIN
    DELETE FROM "{matview_name}";
    INSERT INTO "{matview_name}" {query};
    UPDATE {MATVIEW_METADATA_TABLE} SET last_refreshed_at = datetime('now') WHERE name = '{matview_name}';
END"""
        )

    return statements


def drop_refresh_triggers_sql(
    matview_name: str,
    source_tables: list[str],
) -> list[str]:
    """
    Generate SQL to drop refresh triggers for a matview.

    Args:
        matview_name: The materialized view
        source_tables: Tables that have triggers

    Returns:
        List of DROP TRIGGER statements
    """
    statements: list[str] = []
    events = ["insert", "update", "delete"]

    for table in source_tables:
        for event in events:
            trigger_name = f"_dp_refresh_{matview_name}_on_{table}_{event}"
            statements.append(f"DROP TRIGGER IF EXISTS {trigger_name}")

    return statements


def is_matview_sql() -> str:
    """
    Generate SQL to check if a table is an emulated matview.

    Returns:
        SELECT query that returns 1 if table is a matview, NULL otherwise
    """
    return f"SELECT 1 FROM {MATVIEW_METADATA_TABLE} WHERE name = :table_name"


def get_matview_metadata_sql() -> str:
    """
    Generate SQL to get matview metadata.

    Returns:
        SELECT query for matview metadata
    """
    return f"""SELECT name, query, refresh_strategy, depends_on, last_refreshed_at, created_at
FROM {MATVIEW_METADATA_TABLE}
WHERE name = :name"""


def list_matviews_sql() -> str:
    """
    Generate SQL to list all emulated matviews.

    Returns:
        SELECT query for all matviews
    """
    return f"""SELECT name, refresh_strategy, last_refreshed_at
FROM {MATVIEW_METADATA_TABLE}
ORDER BY name"""


def infer_columns_from_query_sql(query: str) -> list[str]:
    """
    Generate SQL statements to infer column schema from query.

    This creates a temporary table, inspects it, then drops it.
    Must be executed as separate statements.

    Args:
        query: The SELECT query

    Returns:
        List of SQL statements
    """
    return [
        f"CREATE TEMP TABLE _dp_schema_probe AS {query} LIMIT 0",
        "PRAGMA table_info(_dp_schema_probe)",
        "DROP TABLE _dp_schema_probe",
    ]
