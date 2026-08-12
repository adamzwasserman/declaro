"""
SQLite migration applier implementation.

SQLite has limited ALTER TABLE support, so some operations require
table reconstruction (create new, copy data, drop old, rename new).

SQL generation is shared with Turso via applier.shared module.
"""

from typing import Any, Literal

from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    columns_from_pragma_rows,
    dry_run_preview,
    enum_population_sql,
    generate_operation_sql,
    generate_sql,
    map_type as _map_type_shared,
    requires_reconstruction,
    single_change_property,
)
from declaro_persistum.errors import NotSupportedError
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Column, Enum, Operation, Procedure, Trigger, View




# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================


def generate_enum_check(column_name: str, enum: Enum) -> str:
    """
    Generate CHECK constraint for enum column (SQLite fallback).

    Args:
        column_name: Column name
        enum: Enum definition

    Returns:
        CHECK constraint SQL
    """
    values = ", ".join(f"'{v}'" for v in enum["values"])
    return f"CHECK ({column_name} IN ({values}))"


def generate_column_sql(
    col_name: str, col_def: dict[str, Any], enums: dict[str, Enum] | None = None
) -> str:
    """
    Generate column definition, handling enum types with CHECK constraints.

    Args:
        col_name: Column name
        col_def: Column definition
        enums: Dict of known enum definitions

    Returns:
        Column SQL definition
    """
    enums = enums or {}
    col_type = col_def.get("type", "text")

    parts = [f'"{col_name}"']

    # Handle enum type reference - SQLite uses TEXT + CHECK
    if col_type.startswith("enum:"):
        enum_name = col_type[5:]  # Remove "enum:" prefix
        parts.append("TEXT")

        if enum_name in enums:
            check_sql = generate_enum_check(col_name, enums[enum_name])
            parts.append(check_sql)
    else:
        parts.append(_map_type(col_type))

    if col_def.get("nullable") is False:
        parts.append("NOT NULL")

    if "default" in col_def:
        parts.append(f"DEFAULT {col_def['default']}")

    return " ".join(parts)


_map_type = _map_type_shared


def generate_create_trigger(table: str, trigger: Trigger) -> str:
    """
    Generate SQLite CREATE TRIGGER SQL.

    Args:
        table: Table name
        trigger: Trigger definition

    Returns:
        SQL statement
    """
    name = trigger["name"]
    timing = trigger.get("timing", "before").upper()
    event = trigger.get("event", "insert")
    for_each = trigger.get("for_each", "row").upper()
    body = trigger.get("body", "")

    # SQLite trigger name convention
    trigger_name = f"{table}_{name}"

    # Handle single event only - SQLite doesn't support multiple events per trigger
    if isinstance(event, list):
        event = event[0]  # Take first event

    event_sql = event.upper()

    sql = f"""CREATE TRIGGER {trigger_name}
{timing} {event_sql}
ON {table}
FOR EACH {for_each}
BEGIN
    {body}
END"""

    return sql


def generate_create_triggers_for_events(table: str, trigger: Trigger) -> list[str]:
    """
    Generate multiple SQLite triggers for multiple events.

    SQLite requires separate triggers for each event type.

    Args:
        table: Table name
        trigger: Trigger definition (may have multiple events)

    Returns:
        List of SQL statements
    """
    event = trigger.get("event", "insert")

    events = [event] if isinstance(event, str) else event

    sqls = []
    for evt in events:
        single_trigger = dict(trigger)
        single_trigger["event"] = evt
        single_trigger["name"] = f"{trigger['name']}_{evt}"
        sqls.append(generate_create_trigger(table, single_trigger))  # type: ignore

    return sqls


def generate_drop_trigger(table: str, trigger_name: str) -> str:
    """
    Generate SQLite DROP TRIGGER SQL.

    Args:
        table: Table name
        trigger_name: Trigger name

    Returns:
        SQL statement
    """
    return f"DROP TRIGGER IF EXISTS {table}_{trigger_name}"


def generate_create_function(procedure: Procedure) -> str:
    """
    SQLite does not support stored procedures.

    Raises:
        NotSupportedError: Always, with helpful alternatives
    """
    raise NotSupportedError(
        f"SQLite does not support stored procedures. "
        f"Function '{procedure.get('name', 'unknown')}' cannot be created.",
        alternatives=[
            "Move the logic to the application layer (Python function)",
            "Use SQLite user-defined functions via connection.create_function()",
            "Switch to PostgreSQL for stored procedure support",
        ],
    )


def generate_create_view(view: View) -> str:
    """
    Generate SQLite CREATE VIEW SQL.

    For materialized views, uses table-based emulation with metadata tracking.

    Args:
        view: View definition

    Returns:
        SQL statement (or semicolon-separated statements for materialized views)
    """
    from declaro_persistum.abstractions.materialized_views import (
        create_matview_sql,
        generate_refresh_trigger_sql,
    )

    name = view["name"]
    query = view["query"]
    materialized = view.get("materialized", False)

    if not materialized:
        return f'CREATE VIEW IF NOT EXISTS "{name}" AS\n{query}'

    # Use table-based emulation for materialized views
    refresh = view.get("refresh", "manual")
    # Map PostgreSQL strategies to SQLite strategies
    if refresh in ("on_demand", "on_commit"):
        refresh = "manual"

    depends_on = view.get("depends_on")
    trigger_sources = view.get("trigger_sources")

    statements = create_matview_sql(
        name=name,
        query=query,
        refresh_strategy=refresh,  # type: ignore[arg-type]
        depends_on=depends_on,
    )

    # Add triggers if trigger-based refresh
    if refresh in ("trigger", "hybrid") and trigger_sources:
        for source in trigger_sources:
            statements.extend(
                generate_refresh_trigger_sql(
                    matview_name=name,
                    source_table=source,
                    query=query,
                )
            )

    return ";\n".join(statements)


def generate_drop_view(name: str) -> str:
    """
    Generate SQLite DROP VIEW SQL.

    Args:
        name: View name

    Returns:
        SQL statement
    """
    return f"DROP VIEW IF EXISTS {name}"
