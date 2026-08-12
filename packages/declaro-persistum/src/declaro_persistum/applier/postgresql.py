"""
PostgreSQL migration applier implementation.

PostgreSQL supports transactional DDL, so all operations are wrapped
in a single transaction for atomic all-or-nothing behavior.
"""

from collections.abc import Callable
from typing import Any, Literal

from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Column, Enum, Operation, Procedure, Trigger, View

# Type for SQL generator functions
SQLGenerator = Callable[..., str]




# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================


def generate_create_enum(enum: Enum) -> str:
    """
    Generate CREATE TYPE ... AS ENUM SQL.

    Args:
        enum: Enum definition

    Returns:
        SQL statement
    """
    name = enum["name"]
    values = ", ".join(f"'{v}'" for v in enum["values"])
    return f"CREATE TYPE {name} AS ENUM ({values})"


def generate_drop_enum(name: str) -> str:
    """
    Generate DROP TYPE SQL.

    Args:
        name: Enum type name

    Returns:
        SQL statement
    """
    return f"DROP TYPE IF EXISTS {name}"


def generate_alter_enum_add_value(name: str, value: str) -> str:
    """
    Generate ALTER TYPE ... ADD VALUE SQL.

    Args:
        name: Enum type name
        value: Value to add

    Returns:
        SQL statement
    """
    return f"ALTER TYPE {name} ADD VALUE '{value}'"


def generate_column_sql(
    col_name: str, col_def: dict[str, Any], enums: set[str] | None = None
) -> str:
    """
    Generate column definition, handling enum types.

    Args:
        col_name: Column name
        col_def: Column definition
        enums: Set of known enum type names

    Returns:
        Column SQL definition
    """
    enums = enums or set()
    col_type = col_def.get("type", "text")

    # Handle enum type reference
    if col_type.startswith("enum:"):
        enum_name = col_type[5:]  # Remove "enum:" prefix
        col_type = enum_name if enum_name in enums else "text"  # Fallback

    parts = [f'"{col_name}"', col_type]

    if col_def.get("nullable") is False:
        parts.append("NOT NULL")

    if "default" in col_def:
        parts.append(f"DEFAULT {col_def['default']}")

    return " ".join(parts)


def generate_trigger_function(table: str, trigger: Trigger) -> str:
    """
    Generate trigger function SQL.

    Args:
        table: Table name
        trigger: Trigger definition

    Returns:
        SQL statement
    """
    name = trigger["name"]
    body = trigger.get("body", "RETURN NEW;")
    func_name = f"{table}_{name}"

    return f"""CREATE OR REPLACE FUNCTION {func_name}()
RETURNS TRIGGER AS $$
BEGIN
    {body}
END;
$$ LANGUAGE plpgsql"""


def generate_create_trigger(table: str, trigger: Trigger) -> str:
    """
    Generate CREATE TRIGGER SQL.

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
    func_name = f"{table}_{name}"

    # Handle multiple events
    event_sql = " OR ".join(e.upper() for e in event) if isinstance(event, list) else event.upper()

    sql = f"CREATE TRIGGER {name}\n{timing} {event_sql}\nON {table}\nFOR EACH {for_each}"

    # Add WHEN clause if present
    if trigger.get("when"):
        sql += f"\nWHEN ({trigger['when']})"

    # Execute function
    if trigger.get("execute"):
        sql += f"\nEXECUTE FUNCTION {trigger['execute']}()"
    else:
        sql += f"\nEXECUTE FUNCTION {func_name}()"

    return sql


def generate_drop_trigger(table: str, trigger_name: str) -> str:
    """
    Generate DROP TRIGGER SQL.

    Args:
        table: Table name
        trigger_name: Trigger name

    Returns:
        SQL statement
    """
    return f"DROP TRIGGER IF EXISTS {trigger_name} ON {table}"


def generate_create_function(procedure: Procedure) -> str:
    """
    Generate CREATE FUNCTION SQL.

    Args:
        procedure: Procedure definition

    Returns:
        SQL statement
    """
    name = procedure["name"]
    language = procedure.get("language", "sql")
    returns = procedure.get("returns", "void")
    body = procedure.get("body", "")
    params = procedure.get("parameters", [])

    # Build parameter list
    param_parts = []
    for p in params:
        param_sql = f"{p['name']} {p['type']}"
        if p.get("default"):
            param_sql += f" DEFAULT {p['default']}"
        param_parts.append(param_sql)
    params_sql = ", ".join(param_parts)

    return f"""CREATE OR REPLACE FUNCTION {name}({params_sql})
RETURNS {returns}
LANGUAGE {language}
AS $$
{body}
$$"""


def generate_drop_function(procedure: Procedure) -> str:
    """
    Generate DROP FUNCTION SQL.

    Args:
        procedure: Procedure definition

    Returns:
        SQL statement
    """
    name = procedure["name"]
    params = procedure.get("parameters", [])
    param_types = ", ".join(p["type"] for p in params)
    return f"DROP FUNCTION IF EXISTS {name}({param_types})"


def generate_create_view(view: View) -> str:
    """
    Generate CREATE VIEW or CREATE MATERIALIZED VIEW SQL.

    Args:
        view: View definition

    Returns:
        SQL statement
    """
    name = view["name"]
    query = view["query"]
    materialized = view.get("materialized", False)

    if materialized:
        return f"CREATE MATERIALIZED VIEW {name} AS\n{query}"
    else:
        return f"CREATE OR REPLACE VIEW {name} AS\n{query}"


def generate_drop_view(name: str, materialized: bool = False) -> str:
    """
    Generate DROP VIEW SQL.

    Args:
        name: View name
        materialized: Whether it's a materialized view

    Returns:
        SQL statement
    """
    if materialized:
        return f"DROP MATERIALIZED VIEW IF EXISTS {name}"
    else:
        return f"DROP VIEW IF EXISTS {name}"


def generate_refresh_materialized_view(name: str, concurrently: bool = False) -> str:
    """
    Generate REFRESH MATERIALIZED VIEW SQL.

    Args:
        name: View name
        concurrently: Whether to refresh concurrently

    Returns:
        SQL statement
    """
    if concurrently:
        return f"REFRESH MATERIALIZED VIEW CONCURRENTLY {name}"
    else:
        return f"REFRESH MATERIALIZED VIEW {name}"


async def validate_concurrent_refresh(
    connection: Any,
    view_name: str,
    *,
    schema_name: str = "public",
) -> None:
    """
    Validate that a materialized view can be refreshed concurrently.

    PostgreSQL requires a unique index on the materialized view for
    REFRESH MATERIALIZED VIEW CONCURRENTLY to work. This function
    checks that requirement and provides a helpful error message.

    Args:
        connection: asyncpg connection object
        view_name: Name of the materialized view
        schema_name: PostgreSQL schema (default: "public")

    Raises:
        ValidationError: If view lacks required unique index
    """
    from declaro_persistum.exceptions import ValidationError
    from declaro_persistum.inspector.postgresql import PostgreSQLInspector

    inspector = PostgreSQLInspector()
    has_unique = await inspector.has_unique_index(connection, view_name, schema_name=schema_name)

    if not has_unique:
        raise ValidationError(
            f"Cannot refresh '{view_name}' concurrently: "
            f"materialized view requires a unique index.\n\n"
            f"  Create one with:\n"
            f"    CREATE UNIQUE INDEX ON {view_name} (column_name)",
            table=view_name,
        )
