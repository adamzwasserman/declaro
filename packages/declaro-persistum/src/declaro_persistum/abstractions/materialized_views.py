"""
Materialized view abstraction using tables for SQLite/Turso.

This provides emulation of PostgreSQL materialized views on SQLite-family
databases using regular tables with metadata tracking.

Pattern follows arrays.py and maps.py abstractions.
"""

import json
from collections.abc import Callable
from typing import Any, Literal

from declaro_persistum.types import Nothing, resolved

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


def create_matview_sql(
    name: str,
    query: str,
    refresh_strategy: RefreshStrategy | Nothing,
    depends_on: list[str] | None | Nothing,
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
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    refresh_strategy = resolved(refresh_strategy, "manual")
    depends_on = resolved(depends_on, None)
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
    # No dependencies is SQL NULL; a list of them is the JSON of that list.
    depends_json = f"'{json.dumps(depends_on)}'" if depends_on else "NULL"

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
    atomic: bool | Nothing,
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
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    return _REFILL[resolved(atomic, True)](name, query)


def _refill_in_place(name: str, query: str) -> list[str]:
    """Empty the table and put the new rows in it, keeping its indexes."""
    return [
        f'DELETE FROM "{name}"',
        f'INSERT INTO "{name}" {query}',
        _stamp_refreshed(name),
    ]


def _rebuild(name: str, query: str) -> list[str]:
    """Drop the table and make it again, which loses its indexes."""
    return [
        f'DROP TABLE IF EXISTS "{name}"',
        f'CREATE TABLE "{name}" AS {query}',
        _stamp_refreshed(name),
    ]


def _stamp_refreshed(name: str) -> str:
    """Record when this view was last filled."""
    return (
        f"UPDATE {MATVIEW_METADATA_TABLE} SET last_refreshed_at = "
        f"datetime('now') WHERE name = '{name}'"
    )


# Which way the rows are replaced. Keeping the table means keeping its
# indexes, so the two are not interchangeable and the caller says which.
_REFILL: dict[bool, Callable[[str, str], list[str]]] = {
    True: _refill_in_place,
    False: _rebuild,
}


def generate_refresh_trigger_sql(
    matview_name: str,
    source_table: str,
    query: str,
    events: list[str] | None | Nothing,
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
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    events = resolved(events, None)
    events = events or ["INSERT", "UPDATE", "DELETE"]
    statements: list[str] = []

    for event in events:
        trigger_name = f"_dp_refresh_{matview_name}_on_{source_table}_{event.lower()}"

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


# WHAT EACH QUESTION ASKS THE REGISTRY, one row per question. These were three
# functions taking nothing and returning a fixed SELECT against the same table,
# so the SQL was the only thing that differed and it is data now.
#
# THE PARAMETER NAME IS IN THE ROW because it is part of the query. The
# is-it-one question binds `:table_name` and the metadata question binds
# `:name`; a caller reading this table sees which, where before it had to read
# three function bodies.
_REGISTRY_QUERIES: dict[str, tuple[str, str]] = {
    "is_matview_sql": (
        f"SELECT 1 FROM {MATVIEW_METADATA_TABLE} WHERE name = :table_name",
        "SQL asking whether a table is an emulated matview. Binds :table_name.",
    ),
    "get_matview_metadata_sql": (
        "SELECT name, query, refresh_strategy, depends_on, last_refreshed_at, "
        f"created_at\nFROM {MATVIEW_METADATA_TABLE}\nWHERE name = :name",
        "SQL for one matview's registry row. Binds :name.",
    ),
    "list_matviews_sql": (
        "SELECT name, refresh_strategy, last_refreshed_at\n"
        f"FROM {MATVIEW_METADATA_TABLE}\nORDER BY name",
        "SQL listing every emulated matview, by name.",
    ),
}


def _asks(question: str) -> Callable[[], str]:
    """Build the function that hands back one registry query."""

    sql, purpose = _REGISTRY_QUERIES[question]

    def ask() -> str:
        return sql

    ask.__name__ = question
    ask.__qualname__ = question
    ask.__doc__ = purpose
    return ask


is_matview_sql = _asks("is_matview_sql")
get_matview_metadata_sql = _asks("get_matview_metadata_sql")
list_matviews_sql = _asks("list_matviews_sql")
