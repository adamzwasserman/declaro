"""
Table reconstruction abstraction for SQLite/Turso Database.

SQLite doesn't support many ALTER TABLE operations natively. The standard workaround
is table reconstruction:

1. Create new table with desired schema
2. Copy all compatible data from old table
3. Drop old table
4. Rename new table to original name

This module provides:
- Pure SQL generation functions (shared between sync/async)
- Separate execution functions for async (SQLite, Turso) and sync (Turso Database/pyturso)
- Support for: alter_column, add_fk, drop_fk, drop_column operations
- Fresh introspection before each reconstruction (no batching optimization)

Reference: https://www.sqlite.org/lang_altertable.html#making_other_kinds_of_table_schema_changes
"""

import logging
from collections.abc import Callable
from typing import Any

from declaro_persistum.types import Column, Operation

logger = logging.getLogger(__name__)


# =============================================================================
# Pure SQL Generation Functions (Shared)
# =============================================================================


def generate_create_table_sql(
    table_name: str,
    columns: dict[str, Column],
) -> str:
    """
    Generate CREATE TABLE statement from column definitions.

    This is a pure function - generates SQL but does not execute it.
    Foreign keys are embedded in column definitions via the "references" field.

    Args:
        table_name: Name of table
        columns: Dict mapping column name to Column definition

    Returns:
        CREATE TABLE SQL statement

    Example:
        >>> cols = {
        ...     "id": {"type": "INTEGER", "primary_key": True},
        ...     "user_id": {"type": "INTEGER", "nullable": False, "references": "users.id"},
        ... }
        >>> sql = generate_create_table_sql("posts", cols)
    """
    col_defs: list[str] = []
    pk_columns: list[str] = []

    for col_name, col_def in columns.items():
        parts = [f'"{col_name}"']

        # Type
        col_type = col_def.get("type", "TEXT")
        parts.append(_map_type(col_type))

        # Primary key (collect for potential composite key)
        if col_def.get("primary_key"):
            pk_columns.append(col_name)
            # Single-column PK can be inline
            if len(columns) == 1 or not any(
                c.get("primary_key") for n, c in columns.items() if n != col_name
            ):
                parts.append("PRIMARY KEY")

        # NOT NULL
        if col_def.get("nullable") is False:
            parts.append("NOT NULL")

        # UNIQUE
        if col_def.get("unique"):
            parts.append("UNIQUE")

        # DEFAULT
        if "default" in col_def:
            default_val = col_def["default"]
            if default_val is None:
                parts.append("DEFAULT NULL")
            elif isinstance(default_val, str) and default_val.startswith("("):
                # SQLite strips one layer of outer parens from expression defaults
                # in PRAGMA table_info. Double-wrap so the round-trip preserves them.
                parts.append(f"DEFAULT ({default_val})")
            else:
                parts.append(f"DEFAULT {default_val}")

        # CHECK
        if "check" in col_def:
            parts.append(f"CHECK ({col_def['check']})")

        # Inline foreign key reference (column-level)
        if "references" in col_def:
            ref = col_def["references"]
            if "." in ref:
                ref_table, ref_col = ref.split(".", 1)
            else:
                ref_table = ref
                ref_col = "id"

            fk_sql = f'REFERENCES "{ref_table}"("{ref_col}")'

            if col_def.get("on_delete"):
                fk_sql += f" ON DELETE {col_def['on_delete'].upper().replace('_', ' ')}"
            if col_def.get("on_update"):
                fk_sql += f" ON UPDATE {col_def['on_update'].upper().replace('_', ' ')}"

            parts.append(fk_sql)

        col_defs.append(" ".join(parts))

    # Add composite primary key if multiple PK columns
    if len(pk_columns) > 1:
        pk_cols = ", ".join(f'"{c}"' for c in pk_columns)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

    columns_sql = ",\n    ".join(col_defs)
    return f'CREATE TABLE "{table_name}" (\n    {columns_sql}\n)'


def generate_data_copy_sql(
    table_from: str,
    table_to: str,
    common_columns: list[str],
) -> str:
    """
    Generate INSERT...SELECT statement for data migration.

    This is a pure function - generates SQL but does not execute it.

    Args:
        table_from: Source table name
        table_to: Destination table name
        common_columns: List of column names that exist in both tables

    Returns:
        INSERT...SELECT SQL statement

    Example:
        >>> sql = generate_data_copy_sql("users", "users_new", ["id", "email"])
        >>> # Returns: INSERT INTO "users_new" (id, email) SELECT id, email FROM "users"
    """
    if not common_columns:
        raise ValueError("No common columns provided for data copy")

    columns_sql = ", ".join(f'"{col}"' for col in common_columns)
    return (
        f'INSERT INTO "{table_to}" ({columns_sql}) '
        f'SELECT {columns_sql} FROM "{table_from}"'
    )


def get_reconstruction_columns(
    current_columns: dict[str, Column],
    operation: Operation,
) -> dict[str, Column]:
    """
    Compute new column definitions based on operation type.

    This is a pure function - no side effects.

    Args:
        current_columns: Current table schema (from introspection)
        operation: The operation being applied

    Returns:
        New column definitions to use for reconstructed table

    Handles:
        - alter_column: Modify column properties
        - drop_column: Remove column from schema
        - add_foreign_key: Add FK reference to column definition
        - drop_foreign_key: Remove FK reference from column definition
    """
    op_type = operation["op"]
    details = operation["details"]
    new_columns = current_columns.copy()

    _OP_HANDLERS: dict[str, Callable[[dict[str, Column], dict[str, Any]], dict[str, Column]]] = {
        "alter_column": _apply_alter_column,
        "drop_column": _apply_drop_column,
        "add_foreign_key": _apply_add_foreign_key,
        "drop_foreign_key": _apply_drop_foreign_key,
    }

    handler = _OP_HANDLERS.get(op_type)
    if handler is not None:
        new_columns = handler(new_columns, details)

    return new_columns


def _apply_alter_column(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Apply alter_column changes to column definitions."""
    column_name = details["column"]
    if column_name not in columns:
        raise ValueError(f"Column '{column_name}' not found in table")

    col_def = columns[column_name].copy()
    changes = details.get("changes", {})

    for key, value in changes.items():
        if key in ("type", "nullable", "default", "unique", "check"):
            # The differ produces {"from": old, "to": new} dicts for changes
            if isinstance(value, dict) and "to" in value:
                value = value["to"]
            if value is None and key == "default":
                col_def.pop("default", None)
            else:
                col_def[key] = value  # type: ignore

    columns[column_name] = col_def
    return columns


def _apply_drop_column(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Remove column from schema."""
    column_name = details["column"]
    if column_name in columns:
        del columns[column_name]
    return columns


def _apply_add_foreign_key(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Add FK reference to column definition."""
    column_name = details["column"]
    references = details["references"]

    if column_name in columns:
        col_def = columns[column_name].copy()
        col_def["references"] = references
        if details.get("on_delete"):
            col_def["on_delete"] = details["on_delete"]  # type: ignore
        if details.get("on_update"):
            col_def["on_update"] = details["on_update"]  # type: ignore
        columns[column_name] = col_def
    return columns


def _apply_drop_foreign_key(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Remove FK reference from column definition."""
    column_name = details["column"]

    if column_name in columns:
        col_def = columns[column_name].copy()
        col_def.pop("references", None)
        col_def.pop("on_delete", None)
        col_def.pop("on_update", None)
        columns[column_name] = col_def
    return columns


# =============================================================================
# Async Execution (SQLite, Turso)
# =============================================================================


async def execute_reconstruction_async(
    connection: Any,
    table_name: str,
    new_columns: dict[str, Column],
    *,
    preserve_data: bool = True,
    manage_foreign_keys: bool = True,
) -> None:
    """
    Execute table reconstruction asynchronously.

    Performs fresh introspection before reconstruction to ensure we have
    the current state (no caching/optimization across multiple operations).

    Args:
        connection: Async database connection (aiosqlite, pyturso)
        table_name: Name of table to reconstruct
        new_columns: New column definitions (full schema with FKs embedded)
        preserve_data: Whether to copy data from old table (default: True)
        manage_foreign_keys: Whether to toggle PRAGMA foreign_keys on/off.
            Set to False when the caller manages FK pragmas externally
            (required when reconstruction runs inside an explicit transaction,
            since PRAGMA foreign_keys inside a transaction may implicitly commit).

    Raises:
        Exception: If reconstruction fails (caller should handle transaction rollback)

    Example:
        >>> new_cols = {
        ...     "id": {"type": "INTEGER", "primary_key": True},
        ...     "email": {"type": "TEXT", "nullable": False},
        ... }
        >>> await execute_reconstruction_async(conn, "users", new_cols)
    """
    # Validate column definitions before any destructive operations
    _validate_columns(table_name, new_columns)

    temp_table = f"{table_name}_new"

    # 1. Fresh introspection - get current state
    current_columns = await _get_table_columns_async(connection, table_name)

    # 2. Get current foreign key state
    fk_enabled = 0
    if manage_foreign_keys:
        fk_cursor = await connection.execute("PRAGMA foreign_keys")
        fk_row = await fk_cursor.fetchone()
        fk_enabled = fk_row[0] if fk_row else 0

    try:
        # 3. Disable foreign keys during reconstruction
        if manage_foreign_keys:
            await connection.execute("PRAGMA foreign_keys = OFF")

        # 4. Create new table with updated schema
        create_sql = generate_create_table_sql(temp_table, new_columns)
        logger.debug(f"Creating temp table: {create_sql}")
        await connection.execute(create_sql)

        # 5. Copy compatible data if requested
        if preserve_data:
            common_columns = [col for col in current_columns if col in new_columns]

            if common_columns:
                copy_sql = generate_data_copy_sql(table_name, temp_table, common_columns)
                logger.debug(f"Copying data: {copy_sql}")
                await connection.execute(copy_sql)
            else:
                logger.warning(
                    f"No common columns between old and new schema for '{table_name}'. "
                    f"No data will be copied."
                )

        # 6. Get indexes to recreate
        indexes = await _get_table_indexes_async(connection, table_name)

        # 7. Drop old table
        await connection.execute(f'DROP TABLE "{table_name}"')

        # 8. Rename temp table to original name
        await connection.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')

        # 9. Recreate indexes
        for index_sql in indexes:
            # Replace temp table name with original in index definitions
            recreate_sql = index_sql.replace(f'"{temp_table}"', f'"{table_name}"')
            logger.debug(f"Recreating index: {recreate_sql}")
            await connection.execute(recreate_sql)

        # 10. Re-enable foreign keys if we managed them
        if manage_foreign_keys and fk_enabled:
            await connection.execute("PRAGMA foreign_keys = ON")

            # Verify foreign key constraints (all tables — catches incoming FK
            # references from other tables that may now be dangling)
            check_cursor = await connection.execute("PRAGMA foreign_key_check")
            violations = await check_cursor.fetchall()
            if violations:
                raise ValueError(
                    f"Foreign key constraint violations after reconstruction: {violations}"
                )

        logger.info(f"Successfully reconstructed table '{table_name}'")

    except Exception as e:
        logger.error(f"Table reconstruction failed for '{table_name}': {e}")
        # Re-enable foreign keys before re-raising
        if manage_foreign_keys:
            try:
                if fk_enabled:
                    await connection.execute("PRAGMA foreign_keys = ON")
            except Exception:
                pass
        raise


# =============================================================================
# Validation (fail-fast before destructive operations)
# =============================================================================


def _validate_columns(table_name: str, columns: dict[str, Column]) -> None:
    """
    Validate column definitions before starting reconstruction.

    Catches invalid column definitions (e.g., non-string types from unprocessed
    differ output) before any destructive database operations begin.

    Raises:
        ValueError: If any column definition is invalid
    """
    for col_name, col_def in columns.items():
        col_type = col_def.get("type", "TEXT")
        if isinstance(col_type, dict):
            raise ValueError(
                f"Invalid column type for '{table_name}'.'{col_name}': "
                f"got dict {col_type} instead of string. "
                f"This usually means differ output was not unwrapped "
                f"(expected 'to' value from {{'from': ..., 'to': ...}} format)."
            )

    # Validate SQL generation succeeds before touching the database
    try:
        generate_create_table_sql(f"{table_name}_validate", columns)
    except Exception as e:
        raise ValueError(
            f"Cannot reconstruct table '{table_name}': "
            f"invalid column definitions - {e}"
        ) from e


# =============================================================================
# Internal Helper Functions
# =============================================================================


async def _get_table_columns_async(connection: Any, table_name: str) -> list[str]:
    """Get list of column names for a table (async)."""
    from declaro_persistum.abstractions.pragma_compat import pragma_table_info

    rows = await pragma_table_info(connection, table_name)
    return [row[1] for row in rows]  # Column name is at index 1


async def _get_table_indexes_async(connection: Any, table_name: str) -> list[str]:
    """
    Get CREATE INDEX statements for all indexes on a table (async).

    Returns list of CREATE INDEX SQL statements.
    """
    from declaro_persistum.abstractions.pragma_compat import pragma_index_list

    index_list = await pragma_index_list(connection, table_name)
    index_sqls = []

    for row in index_list:
        # row format: (seq, name, unique, origin, partial)
        index_name = row[1]
        origin = row[3]

        # Skip auto-generated indexes (UNIQUE/PRIMARY KEY constraints)
        if origin in ("u", "pk"):
            continue

        # Get CREATE INDEX statement from sqlite_master
        cursor = await connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            (index_name,),
        )
        result = await cursor.fetchone()

        if result and result[0]:
            index_sqls.append(result[0])

    return index_sqls


def _map_type(type_str: str) -> str:
    """Map generic types to SQLite types."""
    if not isinstance(type_str, str):
        return "TEXT"
    type_lower = type_str.lower()

    # SQLite type affinity mapping
    if "int" in type_lower:
        return "INTEGER"
    elif type_lower in ("text", "varchar", "char", "string") or type_lower.startswith("varchar("):
        return "TEXT"
    elif type_lower in ("boolean", "bool"):
        return "INTEGER"
    elif type_lower in ("uuid", "timestamptz", "timestamp", "date", "datetime"):
        return "TEXT"
    elif type_lower in ("jsonb", "json"):
        return "TEXT"
    elif type_lower in ("float", "double", "real", "float4", "float8") or type_lower.startswith(
        "numeric"
    ):
        return "REAL"
    elif type_lower in ("blob", "bytea"):
        return "BLOB"
    else:
        return type_str.upper()
