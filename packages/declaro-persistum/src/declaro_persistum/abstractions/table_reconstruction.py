"""
Table reconstruction abstraction for SQLite/Turso Database.

SQLite doesn't support ALTER COLUMN natively. The only way to change column
definitions (type, nullability, default value) is to reconstruct the table:

1. Create new table with desired schema
2. Copy all compatible data from old table
3. Drop old table
4. Rename new table to original name

This module provides safe, transactional table reconstruction while preserving:
- Data integrity (within transaction)
- Foreign key relationships (temporarily disabled during reconstruction)
- Indexes and constraints (recreated on new table)

Reference: https://www.sqlite.org/lang_altertable.html#making_other_kinds_of_table_schema_changes
"""

import logging
from typing import Any

from declaro_persistum.abstractions.pragma_compat import (
    create_index_statements,
    table_column_names,
)
from declaro_persistum.applier.shared import sqlite_type
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.types import Column

logger = logging.getLogger(__name__)



@helper
def columns_to_copy(stored: list[str], declared: list[str]) -> list[tuple[str, str]]:
    """Pairs of (name in the old table, name in the new one). Pure.

    SQLITE FOLDS IDENTIFIER CASE AND PYTHON DOES NOT, and the rebuild used to
    decide with `[col for col in stored if col in declared]`. A column stored
    as `Note` and declared as `note` is one column for every query anyone can
    write, and was not in that intersection.

    MEASURED 2026-08-17:

        stored columns:            ['id', 'Note']
        declared:                  {'id': ..., 'note': ...}
        rows after reconstruction: [(1, None)]

    The row survived and the value did not.

    NOTHING WARNED. The rebuild logs "No common columns" only when the
    intersection is EMPTY; `id` matched, so the copy went ahead quietly missing
    one column. The loud case is the one you would notice anyway, because the
    table comes out empty.

    Both spellings are returned because both are needed: the SELECT reads the
    old table, which knows the column by its stored name, and the INSERT writes
    the new one, which knows it by the declared name.

    First match wins if a schema somehow declares two columns differing only in
    case. SQLite would refuse to create that table, so the situation cannot
    survive to be a question.
    """
    by_folded: dict[str, str] = {}
    for name in declared:
        by_folded.setdefault(name.lower(), name)
    return [(name, by_folded[name.lower()]) for name in stored if name.lower() in by_folded]


@boundary
async def reconstruct_table(
    connection: Any,
    table_name: str,
    new_columns: dict[str, Column],
    *,
    preserve_data: bool = True,
    manage_foreign_keys: bool = True,
) -> None:
    """
    Reconstruct a table with new column definitions.

    This is the only way to alter column properties in SQLite (type, nullability,
    default values). The operation is wrapped in a transaction for safety.

    Args:
        connection: Database connection (async for SQLite/Turso)
        table_name: Name of table to reconstruct
        new_columns: New column definitions (full schema)
        preserve_data: Whether to copy data from old table (default: True)
        manage_foreign_keys: Whether to toggle PRAGMA foreign_keys on/off.
            Set to False when the caller manages FK pragmas externally
            (required when reconstruction runs inside an explicit transaction,
            since PRAGMA foreign_keys inside a transaction may implicitly commit).

    Raises:
        Exception: If reconstruction fails (transaction will be rolled back)

    Example:
        >>> new_cols = {
        ...     "id": {"type": "INTEGER", "primary_key": True},
        ...     "email": {"type": "TEXT", "nullable": False},  # Changed from nullable
        ... }
        >>> await reconstruct_table(conn, "users", new_cols)
    """
    # One definition, in migrations.py, shared with the recovery that reads
    # these names back. It was a literal here and a literal there, and a change
    # to either is silent: reconstruction keeps working, recovery keeps finding
    # nothing, and a table vanishes after a crash.
    from declaro_persistum.migrations import temp_table_name

    temp_table = temp_table_name(table_name)
    fk_enabled = 0

    try:
        if manage_foreign_keys:
            # 1. Get current foreign key state
            fk_cursor = await connection.execute("PRAGMA foreign_keys")
            fk_row = await fk_cursor.fetchone()
            fk_enabled = fk_row[0] if fk_row else 0

            # 2. Disable foreign keys during reconstruction
            await connection.execute("PRAGMA foreign_keys = OFF")

        # 3. Drop any leftover _new table from a previous failed reconstruction
        await connection.execute(f'DROP TABLE IF EXISTS "{temp_table}"')

        # 4. Create new table with updated schema
        # THE TABLE-LEVEL CONSTRAINTS COME ALONG. A rebuild is handed
        # columns, and a table-level constraint is not on a column, so the
        # new table was created without it: declared, created, read back, and
        # gone the moment an unrelated column changed type. Measured
        # 2026-08-19, the third constraint kind this rebuild lost after the
        # unique and the foreign key.
        table_constraints = await table_constraints_of(connection, table_name)
        create_sql = _generate_create_table_sql(
            temp_table, new_columns, table_constraints
        )
        logger.debug(f"Creating temp table: {create_sql}")
        await connection.execute(create_sql)

        # 4. Copy compatible data if requested
        if preserve_data:
            # Get intersection of old and new column names
            old_columns = await table_column_names(connection, table_name)
            common_columns = columns_to_copy(old_columns, list(new_columns))

            if common_columns:
                read_sql = ", ".join(f'"{old}"' for old, _ in common_columns)
                write_sql = ", ".join(f'"{new}"' for _, new in common_columns)
                copy_sql = (
                    f'INSERT INTO "{temp_table}" ({write_sql}) '
                    f'SELECT {read_sql} FROM "{table_name}"'
                )
                logger.debug(f"Copying data: {copy_sql}")
                await connection.execute(copy_sql)
            else:
                logger.warning(
                    f"No common columns between old and new schema for '{table_name}'. "
                    f"No data will be copied."
                )

        # 5. Get indexes and constraints to recreate
        indexes = await create_index_statements(connection, table_name)

        # 6. Drop old table
        await connection.execute(f'DROP TABLE "{table_name}"')

        # 7. Rename temp table to original name
        await connection.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')

        # 8. Recreate indexes (substituting new table name)
        for index_sql in indexes:
            # Replace old table name with new in index definitions
            recreate_sql = index_sql.replace(f'"{temp_table}"', f'"{table_name}"')
            logger.debug(f"Recreating index: {recreate_sql}")
            await connection.execute(recreate_sql)

        # 9. THE RESTORE IS NOT DONE HERE, AND CANNOT BE.
        #
        # This used to be `PRAGMA foreign_keys = ON`, and it never took effect.
        # SQLite IGNORES that pragma while a transaction is open, and does not
        # raise, so the line read as though it worked. Measured 2026-08-17:
        #
        #     inside an open transaction   PRAGMA foreign_keys = ON -> still 0
        #     after the transaction ends   -> 1
        #
        # This function must not commit: the applier commits once for all
        # operations, so committing here would break its all-or-nothing
        # transaction. The restore therefore belongs to whoever commits, and
        # `applier/sqlite.py` and `applier/turso.py` both do it after theirs.
        #
        # A caller using this function directly owns the restore, and
        # `test_the_fk_restore_actually_lands.py` says so.
        if manage_foreign_keys and fk_enabled:

            # Verify foreign key constraints
            check_cursor = await connection.execute(f'PRAGMA foreign_key_check("{table_name}")')
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
            except Exception as fk_exc:
                # The original failure is about to be re-raised, but this one
                # leaves the connection enforcing nothing. A caller that
                # catches the reconstruction error and carries on would write
                # against unchecked constraints and never know.
                logger.error(
                    "Could not re-enable foreign keys after the failed "
                    "reconstruction of '%s': %s. THIS CONNECTION NOW HAS "
                    "FOREIGN KEY ENFORCEMENT OFF.",
                    table_name, fk_exc,
                )
        raise


@helper
async def alter_column_nullability(
    connection: Any,
    table_name: str,
    column_name: str,
    nullable: bool,
) -> None:
    """
    Change nullability of a column (SET NOT NULL / DROP NOT NULL).

    Args:
        connection: Database connection
        table_name: Table name
        column_name: Column to modify
        nullable: True to allow NULL, False to disallow

    Example:
        >>> await alter_column_nullability(conn, "users", "email", False)
    """
    # Get current schema
    columns = await _get_full_table_schema(connection, table_name)

    if column_name not in columns:
        raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")

    # Update nullability
    columns[column_name]["nullable"] = nullable

    # Reconstruct table
    await reconstruct_table(connection, table_name, columns)


@helper
async def alter_column_type(
    connection: Any,
    table_name: str,
    column_name: str,
    new_type: str,
) -> None:
    """
    Change column type.

    Warning: This may result in data loss if types are incompatible.
    SQLite is permissive with type conversions.

    Args:
        connection: Database connection
        table_name: Table name
        column_name: Column to modify
        new_type: New column type (e.g., "TEXT", "INTEGER")

    Example:
        >>> await alter_column_type(conn, "users", "age", "TEXT")
    """
    # Get current schema
    columns = await _get_full_table_schema(connection, table_name)

    if column_name not in columns:
        raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")

    # Update type
    columns[column_name]["type"] = new_type

    # Reconstruct table
    await reconstruct_table(connection, table_name, columns)


@helper
async def alter_column_default(
    connection: Any,
    table_name: str,
    column_name: str,
    default_value: str | None,
) -> None:
    """
    Change or remove column default value.

    Args:
        connection: Database connection
        table_name: Table name
        column_name: Column to modify
        default_value: New default value SQL expression, or None to remove

    Example:
        >>> await alter_column_default(conn, "users", "status", "'active'")
        >>> await alter_column_default(conn, "users", "status", None)
    """
    # Get current schema
    columns = await _get_full_table_schema(connection, table_name)

    if column_name not in columns:
        raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")

    # Update default
    if default_value is None:
        columns[column_name].pop("default", None)
    else:
        columns[column_name]["default"] = default_value

    # Reconstruct table
    await reconstruct_table(connection, table_name, columns)


# =============================================================================
# Internal Helper Functions
# =============================================================================




@boundary
async def _get_full_table_schema(connection: Any, table_name: str) -> dict[str, Column]:
    """
    Extract full column schema from existing table.

    Returns dict mapping column name to Column definition, including
    UNIQUE constraints and foreign key references.
    """
    from declaro_persistum.abstractions.pragma_compat import (
        pragma_foreign_key_list,
        pragma_index_list,
        pragma_table_info,
    )

    rows = await pragma_table_info(connection, table_name)
    columns: dict[str, Column] = {}

    for row in rows:
        cid, name, type_str, notnull, dflt_value, pk = row

        col_def: Column = {
            "type": type_str or "TEXT",
            "nullable": not bool(notnull),
        }

        if pk:
            col_def["primary_key"] = True

        if dflt_value is not None:
            col_def["default"] = dflt_value

        columns[name] = col_def

    # Detect inline UNIQUE constraints (auto-generated indexes with origin='u')
    index_list = await pragma_index_list(connection, table_name)
    for idx_row in index_list:
        # (seq, name, unique, origin, partial)
        if idx_row[3] == "u" and idx_row[2]:
            cursor = await connection.execute(f'PRAGMA index_info("{idx_row[1]}")')
            idx_info = await cursor.fetchall()
            if len(idx_info) == 1:  # Single-column unique constraint
                col_name = idx_info[0][2]
                if col_name in columns:
                    columns[col_name]["unique"] = True

    # Detect foreign key constraints
    fk_list = await pragma_foreign_key_list(connection, table_name)
    for fk_row in fk_list:
        # (id, seq, table, from, to, on_update, on_delete, match)
        from_col, ref_table, ref_col = fk_row[3], fk_row[2], fk_row[4]
        on_delete, on_update = fk_row[6], fk_row[5]
        if from_col in columns:
            columns[from_col]["references"] = f"{ref_table}.{ref_col}"
            if on_delete and on_delete.upper() not in ("NO ACTION", "NONE", ""):
                columns[from_col]["on_delete"] = on_delete.lower()
            if on_update and on_update.upper() not in ("NO ACTION", "NONE", ""):
                columns[from_col]["on_update"] = on_update.lower()

    # A CHECK IS IN NO PRAGMA, so this function did not read one and every
    # rebuild dropped it, on both engines. Measured 2026-08-19: a table with
    # `"age" INTEGER CHECK (age > 0)` came back as `"age" INTEGER` after a
    # rebuild triggered by an unrelated column.
    #
    # `checks_from_ddl` already exists for exactly this, and the inspectors
    # have used it since 2026-08-17. It parses the stored CREATE TABLE, which
    # is the only place a CHECK is recorded.
    from declaro_persistum.inspector.shared import checks_from_ddl

    cursor = await connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    ddl_rows = await cursor.fetchall()
    if ddl_rows and ddl_rows[0][0]:
        for name, expression in checks_from_ddl(ddl_rows[0][0]).items():
            if name in columns:
                columns[name]["check"] = expression

    return columns


@boundary
async def table_constraints_of(connection: Any, table_name: str) -> dict[str, Any]:
    """The named table-level constraints, for a rebuild to write back.

    A REBUILD DROPPED THEM. `reconstruct_table` is handed columns, and a
    table-level constraint is not on a column, so the new table was created
    without it. Measured 2026-08-19: a constraint declared, created, read back
    and then gone the moment an unrelated column changed type.

    Same shape as the unique and the foreign key that this file already lost
    on a rebuild, and the third time the answer is that the rebuild has to
    read the whole table rather than part of it.
    """
    from declaro_persistum.inspector.shared import table_constraints_from_ddl

    cursor = await connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    rows = await cursor.fetchall()
    if rows and rows[0][0]:
        return dict(table_constraints_from_ddl(rows[0][0]))
    return {}




@helper
def _generate_create_table_sql(
    table_name: str,
    columns: dict[str, Column],
    constraints: dict[str, dict[str, str]] | None = None,
) -> str:
    """
    Generate CREATE TABLE statement from column definitions.

    Args:
        table_name: Name of table
        columns: Dict mapping column name to Column definition

    Returns:
        CREATE TABLE SQL statement
    """
    col_defs: list[str] = []
    pk_columns: list[str] = []

    for col_name, col_def in columns.items():
        parts = [f'"{col_name}"']

        # Type
        col_type = col_def.get("type", "TEXT")
        parts.append(_map_type(col_type))

        # Primary key
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
            parts.append(f"DEFAULT {col_def['default']}")

        # CHECK
        if "check" in col_def:
            parts.append(f"CHECK ({col_def['check']})")

        # Foreign key reference
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

    # Written through the shared clause so a rebuilt table cannot spell a
    # constraint differently from the create that first made it.
    from declaro_persistum.applier.shared import constraint_clause

    for name, definition in (constraints or {}).items():
        col_defs.append(constraint_clause(name, definition))

    columns_sql = ",\n    ".join(col_defs)
    return f'CREATE TABLE "{table_name}" (\n    {columns_sql}\n)'


# SQLite spelling. This was a byte-for-byte copy of `applier.shared.sqlite_type`.
# It carries no non-string guard and raises AttributeError on a dict, which is
# how it has always behaved; `abstractions/reconstruction._map_type` returns
# TEXT for that input. The divergence is recorded, not resolved here.
_map_type = sqlite_type
