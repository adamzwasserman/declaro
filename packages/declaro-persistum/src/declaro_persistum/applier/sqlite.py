"""Apply migration operations to SQLite. Functions, not a class.

`SQLiteApplier` was stateless and held one real method, `apply()`. The SQL
GENERATORS beside it — generate_column_sql, generate_create_view,
generate_create_trigger and the rest — were already module-level functions.
So the pure/impure split was already drawn, and drawn in the right place; the
class simply sat around the impure half for no reason.

SQLite has limited ALTER TABLE support, so some operations cannot be expressed
as an ALTER at all and go through table reconstruction instead: create the new
shape, copy the data across, drop the old, rename the new.

SQL generation is shared with Turso through `applier.shared`.
"""

from typing import Any, Literal

from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    dry_run_preview,
    generate_operation_sql,
    requires_reconstruction,
    single_change_property,
)
from declaro_persistum.applier.shared import (
    map_type as _map_type_shared,
)
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Enum, Operation, Trigger, View


def get_dialect() -> str:
    """Return dialect identifier."""
    return "sqlite"


def get_transaction_mode() -> Literal["all_or_nothing", "per_operation"]:
    """SQLite supports transactional DDL."""
    return "all_or_nothing"


async def apply(
    connection: Any,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool = False,
) -> ApplyResult:
    """
    Apply migration operations within a transaction.

    SQLite DDL is transactional, allowing safe rollback on failure.
    Uses per-operation execution with reconstruction for unsupported operations.
    """
    if dry_run:
        return dry_run_preview(operations, execution_order)

    executed: list[str] = []

    try:
        # Enable foreign keys and start transaction
        await connection.execute("PRAGMA foreign_keys = ON")

        # Per-operation execution
        for op_idx in execution_order:
            operation = operations[op_idx]

            try:
                if requires_reconstruction(operation):
                    # Execute with reconstruction
                    await _execute_with_reconstruction(connection, operation)
                    executed.append(f"Table reconstruction for {operation['table']}")
                else:
                    # Direct SQL execution
                    sql = generate_operation_sql(operation)
                    for statement in sql.split(";"):
                        statement = statement.strip()
                        if statement:
                            await connection.execute(statement)
                    executed.append(sql)

            except Exception as e:
                await connection.rollback()
                raise MigrationError(
                    f"Failed to execute operation",
                    operation=operation,
                    original_error=e,
                ) from e

        await connection.commit()

        return {
            "success": True,
            "executed_sql": executed,
            "operations_applied": len(executed),
            "error": None,
            "error_operation": None,
        }

    except MigrationError:
        raise
    except Exception as e:
        await connection.rollback()
        raise MigrationError(
            f"Migration failed: {e}",
            original_error=e,
        ) from e


async def _execute_with_reconstruction(
    connection: Any, operation: Operation
) -> None:
    """
    Execute an operation using table reconstruction (async).

    Fresh introspection is performed before each reconstruction to ensure
    we have the latest schema state. Uses specialized functions for
    single-property changes when possible.
    """
    from declaro_persistum.abstractions.table_reconstruction import (
        _get_full_table_schema,
        alter_column_default,
        alter_column_nullability,
        alter_column_type,
        reconstruct_table,
    )

    table = operation["table"]

    # Fresh introspection for current state (includes FKs + unique constraints)
    columns = await _get_full_table_schema(connection, table)

    # Apply reconstruction changes (pure)
    columns = apply_reconstruction_changes(columns, operation)

    # Use specialized functions for single-property alter_column changes
    single = single_change_property(operation)
    if single is not None:
        change_key, val = single
        column = operation["details"]["column"]
        _SPECIALIZED = {
            "nullable": alter_column_nullability,
            "type": alter_column_type,
            "default": alter_column_default,
        }
        handler = _SPECIALIZED.get(change_key)
        if handler is not None:
            await handler(connection, table, column, val)
            return

    # General reconstruction
    await reconstruct_table(connection, table, columns)






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
