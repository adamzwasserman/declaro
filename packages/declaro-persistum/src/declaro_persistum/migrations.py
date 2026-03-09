"""
Automatic schema migration support for declaro_persistum.

Loads target schema from Pydantic models, introspects current database state,
computes diff, and applies changes.

Usage:
    await apply_migrations_async(pool, dialect, schema_path)
"""

import logging
from pathlib import Path
from typing import Any, Union

from declaro_persistum.applier.protocol import create_applier
from declaro_persistum.differ import diff
from declaro_persistum.inspector.protocol import create_inspector
from declaro_persistum.inspector.shared import normalize_schema_for_sqlite
from declaro_persistum.pydantic_loader import load_models_from_module
from declaro_persistum.abstractions.enums import expand_schema_enums

_SQLITE_DIALECTS = frozenset(("sqlite", "turso", "libsql"))

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

    # Normalize target schema types to match what SQLite introspection reports
    if dialect in _SQLITE_DIALECTS:
        target_schema = normalize_schema_for_sqlite(target_schema)

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
