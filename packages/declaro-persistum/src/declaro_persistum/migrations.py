"""
Automatic schema migration support for declaro_persistum.

Provides both async and sync migration functions that:
1. Load target schema from Pydantic models
2. Introspect current database state
3. Compute diff and apply changes

Usage:
    # Async
    await apply_migrations_async(pool, dialect, schema_path)

    # Sync
    apply_migrations_sync(pool, dialect, schema_path)
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Union

from declaro_persistum.applier.protocol import create_applier
from declaro_persistum.differ import diff
from declaro_persistum.inspector.protocol import create_inspector
from declaro_persistum.pydantic_loader import load_models_from_module
from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.types import ApplyResult, Operation, Schema

logger = logging.getLogger(__name__)


async def apply_migrations_async(
    pool: Any,
    dialect: str,
    schema_path: Union[str, Path],
    *,
    expand_enums: bool = True,
) -> dict[str, Any]:
    """
    Apply automatic schema migrations asynchronously.

    Loads schema from Pydantic models, compares with actual database,
    and applies changes if differences are found.

    Args:
        pool: The database connection pool (async)
        dialect: Database dialect ('sqlite', 'turso', 'libsql', 'postgresql')
        schema_path: Path to Python module containing Pydantic models
        expand_enums: Whether to expand Literal types to lookup tables (default True)

    Returns:
        Dict with migration result:
        - success: bool
        - operations_applied: int
        - tables_in_schema: int
        - tables_in_database: int
        - error: str | None

    Raises:
        Exception: If migration fails critically (logs error but may not crash)
    """
    schema_path = Path(schema_path)

    if not schema_path.exists():
        logger.warning(f"Schema file not found: {schema_path}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "error": f"Schema file not found: {schema_path}",
        }

    logger.info(f"Loading schema from {schema_path}")
    target_schema = load_models_from_module(schema_path)

    if not target_schema:
        logger.warning("No tables found in schema models")
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "error": None,
        }

    logger.info(f"Loaded {len(target_schema)} tables from schema models")

    # Expand Literal types to lookup tables + FK constraints
    if expand_enums:
        target_schema = expand_schema_enums(target_schema)
        logger.info(
            f"Expanded schema to {len(target_schema)} tables (including enum lookup tables)"
        )

    # Introspect current database state
    inspector = create_inspector(dialect)
    async with pool.acquire() as conn:
        current_schema = await inspector.introspect(conn)

    logger.info(f"Introspected {len(current_schema)} tables from database")

    # Compute diff
    diff_result = diff(current_schema, target_schema)

    if not diff_result["operations"]:
        logger.info("Schema is up to date - no migrations needed")
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "error": None,
        }

    # Log planned operations
    logger.info(f"Found {len(diff_result['operations'])} schema differences")
    for op in diff_result["operations"]:
        logger.info(f"  - {op['op']} on table {op.get('table', 'N/A')}")

    # Check for ambiguities
    if diff_result.get("ambiguities"):
        logger.warning(f"Ambiguous changes detected: {diff_result['ambiguities']}")
        logger.warning("Skipping migration - manual intervention required")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "error": f"Ambiguous changes: {diff_result['ambiguities']}",
        }

    # Apply migrations
    applier = create_applier(dialect)
    async with pool.acquire() as conn:
        result = await applier.apply(
            conn, diff_result["operations"], diff_result["execution_order"]
        )

    if result["success"]:
        logger.info(f"Successfully applied {result['operations_applied']} migrations")
        for sql in result["executed_sql"]:
            logger.debug(f"  Executed: {sql}")
        return {
            "success": True,
            "operations_applied": result["operations_applied"],
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema) + result["operations_applied"],
            "error": None,
        }
    else:
        error_msg = result.get("error", "Unknown error")
        logger.error(f"Migration failed: {error_msg}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "error": error_msg,
        }


def apply_migrations_sync(
    pool: Any,
    dialect: str,
    schema_path: Union[str, Path],
    *,
    expand_enums: bool = True,
) -> dict[str, Any]:
    """
    Apply automatic schema migrations synchronously.

    This is a sync wrapper around the async migration logic. It uses
    asyncio.run() internally for async operations (introspection, apply).

    Args:
        pool: The database connection pool (sync - SyncSQLitePool, SyncTursoPool, etc.)
        dialect: Database dialect ('sqlite', 'turso', 'libsql')
        schema_path: Path to Python module containing Pydantic models
        expand_enums: Whether to expand Literal types to lookup tables (default True)

    Returns:
        Dict with migration result:
        - success: bool
        - operations_applied: int
        - tables_in_schema: int
        - tables_in_database: int
        - error: str | None
    """
    schema_path = Path(schema_path)

    if not schema_path.exists():
        logger.warning(f"Schema file not found: {schema_path}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "error": f"Schema file not found: {schema_path}",
        }

    logger.info(f"Loading schema from {schema_path}")
    target_schema = load_models_from_module(schema_path)

    if not target_schema:
        logger.warning("No tables found in schema models")
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "error": None,
        }

    logger.info(f"Loaded {len(target_schema)} tables from schema models")

    # Expand Literal types to lookup tables + FK constraints
    if expand_enums:
        target_schema = expand_schema_enums(target_schema)
        logger.info(
            f"Expanded schema to {len(target_schema)} tables (including enum lookup tables)"
        )

    # Introspect current database state using sync connection
    current_schema = _introspect_sync(pool, dialect)
    logger.info(f"Introspected {len(current_schema)} tables from database")

    # Compute diff
    diff_result = diff(current_schema, target_schema)

    if not diff_result["operations"]:
        logger.info("Schema is up to date - no migrations needed")
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "error": None,
        }

    # Log planned operations
    logger.info(f"Found {len(diff_result['operations'])} schema differences")
    for op in diff_result["operations"]:
        logger.info(f"  - {op['op']} on table {op.get('table', 'N/A')}")

    # Check for ambiguities
    if diff_result.get("ambiguities"):
        logger.warning(f"Ambiguous changes detected: {diff_result['ambiguities']}")
        logger.warning("Skipping migration - manual intervention required")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "error": f"Ambiguous changes: {diff_result['ambiguities']}",
        }

    # Apply migrations using sync connection
    result = _apply_sync(pool, dialect, diff_result["operations"], diff_result["execution_order"], target_schema)

    if result["success"]:
        logger.info(f"Successfully applied {result['operations_applied']} migrations")
        for sql in result.get("executed_sql", []):
            logger.debug(f"  Executed: {sql}")
        return {
            "success": True,
            "operations_applied": result["operations_applied"],
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema) + result["operations_applied"],
            "error": None,
        }
    else:
        error_msg = result.get("error", "Unknown error")
        logger.error(f"Migration failed: {error_msg}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "error": error_msg,
        }


def _introspect_sync(pool: Any, dialect: str) -> Schema:
    """
    Introspect database schema using sync connection.

    Uses direct SQL queries instead of async inspector.
    """
    with pool.acquire() as conn:
        if dialect in ("sqlite", "turso"):
            return _introspect_sqlite_sync(conn)
        else:
            raise ValueError(f"Sync introspection not supported for dialect: {dialect}")


def _introspect_sqlite_sync(conn: Any) -> Schema:
    """
    Introspect SQLite/Turso schema synchronously.

    Returns schema dict compatible with declaro_persistum differ.
    """
    schema: Schema = {}

    # Get all tables
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [row[0] for row in cursor.fetchall()]

    for table_name in tables:
        # Get column info
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        columns = {}
        primary_keys = []

        for row in cursor.fetchall():
            col_name = row[1]
            col_type = row[2] or "TEXT"
            not_null = bool(row[3])
            default_value = row[4]
            is_pk = bool(row[5])

            columns[col_name] = {
                "type": col_type.upper(),
                "nullable": not not_null,
                "primary_key": is_pk,
            }

            if default_value is not None:
                columns[col_name]["default"] = default_value

            if is_pk:
                primary_keys.append(col_name)

        # Get foreign keys
        cursor = conn.execute(f"PRAGMA foreign_key_list({table_name})")
        foreign_keys = []
        for row in cursor.fetchall():
            foreign_keys.append({
                "column": row[3],
                "references_table": row[2],
                "references_column": row[4],
                "on_delete": row[6] if row[6] else "NO ACTION",
                "on_update": row[5] if row[5] else "NO ACTION",
            })

        # Get indexes - must be a dict keyed by index name
        cursor = conn.execute(f"PRAGMA index_list({table_name})")
        indexes: dict[str, Any] = {}
        for row in cursor.fetchall():
            idx_name = row[1]
            is_unique = bool(row[2])
            origin = row[3] if len(row) > 3 else "c"  # origin: c=created, u=unique, pk=primary key

            # Skip auto-created indexes (primary keys, unique constraints)
            if origin in ("pk", "u") or idx_name.startswith("sqlite_autoindex"):
                continue

            # Get index columns
            idx_cursor = conn.execute(f"PRAGMA index_info({idx_name})")
            idx_columns = [r[2] for r in idx_cursor.fetchall()]

            index: dict[str, Any] = {"columns": idx_columns}
            if is_unique:
                index["unique"] = True

            indexes[idx_name] = index

        table_def: dict[str, Any] = {
            "columns": columns,
            "primary_key": primary_keys,
        }

        if foreign_keys:
            table_def["foreign_keys"] = foreign_keys
        if indexes:
            table_def["indexes"] = indexes

        schema[table_name] = table_def  # type: ignore[assignment]

    return schema


def _apply_sync(
    pool: Any,
    dialect: str,
    operations: list[Operation],
    execution_order: list[int],
    target_schema: Schema | None = None,
) -> ApplyResult:
    """
    Apply migration operations using sync connection.

    Delegates to the applier's apply_sync() method for execution.

    Args:
        pool: Sync connection pool
        dialect: Database dialect
        operations: List of operations
        execution_order: Order to execute operations
        target_schema: Target schema (used for enum value population)

    Returns:
        ApplyResult dict with success status, executed SQL, and error info
    """
    applier = create_applier(dialect)

    with pool.acquire() as conn:
        return applier.apply_sync(
            conn,
            operations,
            execution_order,
            target_schema=target_schema,
        )
