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
from collections.abc import Awaitable, Callable
from typing import Any

from declaro_persistum.abstractions.pragma_compat import (
    create_index_statements,
    table_column_names,
)
from declaro_persistum.applier.shared import (
    copy_the_shared_columns,
    refuse_the_violations,
    refuse_unwritable_column_types,
    sqlite_default,
    sqlite_type,
)
from declaro_persistum.applier.spelling import constraint_clause
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.types import NOTHING, Column, Nothing, Operation, changed_to, resolved

logger = logging.getLogger(__name__)


# =============================================================================
# Pure SQL Generation Functions (Shared)
# =============================================================================


@helper
def generate_create_table_sql(
    table_name: str,
    columns: dict[str, Column],
    constraints: dict[str, dict[str, str]] | None | Nothing,
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
        >>> sql = generate_create_table_sql("posts", cols, constraints=NOTHING)
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    constraints = resolved(constraints, None)
    # THE REFUSAL BELONGS TO THE RENDERER, NOT ONLY THE ORCHESTRATOR.
    # `_validate_columns` ran in the rebuild path, so a caller who rendered a
    # CREATE TABLE directly got an AttributeError from inside a type mapper
    # naming no column. Both rebuilders now refuse the same way.
    refuse_unwritable_column_types(table_name, columns)

    pk_columns = [
        col_name for col_name, col_def in columns.items() if col_def.get("primary_key")
    ]

    # A KEY OF ONE COLUMN IS WRITTEN ON THAT COLUMN; A KEY OF SEVERAL BECOMES
    # A TABLE-LEVEL CLAUSE. The test this replaces asked whether any other
    # column carried a key, which is the same question asked one column at a
    # time.
    inline_key = frozenset(name for name in pk_columns if len(pk_columns) == 1)

    col_defs = [
        _column_clause(col_name, col_def, col_name in inline_key)
        for col_name, col_def in columns.items()
    ]

    composite_key = [
        f"PRIMARY KEY ({pk_cols})"
        for pk_cols in [", ".join(f'"{c}"' for c in pk_columns)]
        if len(pk_columns) > 1
    ]

    # Through the shared clause, so the two reconstruction modules and the
    # two create renderers cannot spell a constraint four different ways.
    constraint_defs = [
        constraint_clause(name, definition)
        for name, definition in (constraints or {}).items()
    ]

    columns_sql = ",\n    ".join([*col_defs, *composite_key, *constraint_defs])
    return f'CREATE TABLE "{table_name}" (\n    {columns_sql}\n)'


@helper
def _column_clause(col_name: str, col_def: Column, inline_key: bool) -> str:
    """One column, as it appears inside a CREATE TABLE.

    Each clause is a comprehension over the entry that carries it, so a column
    definition that says nothing about a property contributes nothing here.
    """
    return " ".join(
        [
            f'"{col_name}"',
            # NO DEFAULT. A column that never said what it is does not become a
            # TEXT column; the loader refuses one, so reaching here means a
            # caller built the definition by hand and left the type out.
            _map_type(col_def["type"]),
            *["PRIMARY KEY" for sole in [inline_key] if sole],
            *["NOT NULL" for value in [col_def.get("nullable")] if value is False],
            *["UNIQUE" for value in [col_def.get("unique")] if value],
            # DEFAULT, WRITTEN BY THE ONE RULE THAT KNOWS IT. This built the
            # clause itself, including its own test for a leading parenthesis
            # and its own double-wrap. `sqlite_default` in the applier carried
            # the same rule, and a rebuild that writes a default differently
            # from the applier is a rebuilt table without the default it was
            # given.
            #
            # The double wrap is the part that must not drift: PRAGMA
            # table_info strips one layer from an expression default, so two go
            # in and one comes back, and that is what makes introspection agree
            # with the schema.
            *[
                sqlite_default(value)
                for key, value in col_def.items()
                if key == "default"
            ],
            *[f"CHECK ({value})" for key, value in col_def.items() if key == "check"],
            *[
                _references_clause(value, col_def)
                for key, value in col_def.items()
                if key == "references"
            ],
        ]
    )


@helper
def _references_clause(reference: str, col_def: Column) -> str:
    """The column-level foreign key, with the actions the column declares."""
    ref_table, dot, named_column = reference.partition(".")

    # "users.id" names the column it points at; "users" means that table's id.
    ref_col = {True: named_column, False: "id"}[bool(dot)]

    # The action is spelled the way SQL spells it -- SET NULL, CASCADE --
    # written here as `applier/spelling.py` writes it rather than as a fourth
    # function doing the same two calls.
    return " ".join(
        [
            f'REFERENCES "{ref_table}"("{ref_col}")',
            *[
                f"ON DELETE {action.upper().replace('_', ' ')}"
                for key, action in col_def.items()
                if key == "on_delete" and action
            ],
            *[
                f"ON UPDATE {action.upper().replace('_', ' ')}"
                for key, action in col_def.items()
                if key == "on_update" and action
            ],
        ]
    )


@helper
def _the_columns_to_copy(common_columns: list[str]) -> list[str]:
    """The columns both tables have, which are the ones the copy names."""
    return common_columns


@helper
def _refuse_an_empty_copy(_common_columns: list[str]) -> list[str]:
    """No column is in both tables, so there is no INSERT to write."""
    raise ValueError("No common columns provided for data copy")


# Whether there is anything to copy. An INSERT naming no column is not SQL,
# and the caller is told rather than handed one.
_COLUMNS_TO_COPY: dict[bool, Callable[[list[str]], list[str]]] = {
    True: _the_columns_to_copy,
    False: _refuse_an_empty_copy,
}


@helper
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
    copied = _COLUMNS_TO_COPY[bool(common_columns)](common_columns)
    columns_sql = ", ".join(f'"{col}"' for col in copied)
    return (
        f'INSERT INTO "{table_to}" ({columns_sql}) '
        f'SELECT {columns_sql} FROM "{table_from}"'
    )


@helper
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
    # AN OPERATION THIS MODULE REWRITES COLUMNS FOR IS APPLIED; ANY OTHER
    # LEAVES THE SCHEMA AS INTROSPECTION FOUND IT. The comprehension holds
    # nought or one handler, and the one that changes nothing stands behind it,
    # so the table is read by subscript and answers only what it declares.
    op_type = operation["op"]
    written = [_OP_HANDLERS[op] for op in [op_type] if op in _OP_HANDLERS]
    return [*written, _columns_as_they_are][0](
        current_columns.copy(), operation["details"]
    )


@helper
def _columns_as_they_are(
    columns: dict[str, Column], _details: dict[str, Any]
) -> dict[str, Column]:
    """An operation that rewrites no column, so the schema stands as it is."""
    return dict(columns)


# The properties `alter_column` may change. A change to anything else is not
# a column property and is left where it was found.
_ALTERABLE = frozenset({"type", "nullable", "default", "unique", "check"})

# What a column says about the table it points at. Dropping the reference
# drops all three together.
_FOREIGN_KEY_KEYS = frozenset({"references", "on_delete", "on_update"})


@helper
def _alter_the_column(
    columns: dict[str, Column], details: dict[str, Any]
) -> dict[str, Column]:
    """Write the changed properties into the column the operation names."""
    column_name = details["column"]

    # The differ produces {"from": old, "to": new} dicts for changes.
    changes = {
        key: changed_to(value)
        for key, value in details.get("changes", {}).items()
        if key in _ALTERABLE
    }

    # A DEFAULT CHANGED TO NOTHING IS A DEFAULT REMOVED, not a default of
    # NULL. It is dropped from the definition rather than written as None.
    removed = {
        key for key, value in changes.items() if key == "default" and value is None
    }

    altered: dict[str, Any] = {
        **{
            key: value
            for key, value in columns[column_name].items()
            if key not in removed
        },
        **{key: value for key, value in changes.items() if key not in removed},
    }
    return {**columns, column_name: altered}  # type: ignore[dict-item]


@helper
def _no_such_column(
    _columns: dict[str, Column], details: dict[str, Any]
) -> dict[str, Column]:
    """The operation names a column this table does not have."""
    raise ValueError(f"Column '{details['column']}' not found in table")


# A rebuild drops the real table, so a column that is not there is refused
# while the table still is.
_COLUMN_TO_ALTER: dict[
    bool, Callable[[dict[str, Column], dict[str, Any]], dict[str, Column]]
] = {
    True: _alter_the_column,
    False: _no_such_column,
}


@helper
def _apply_alter_column(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Apply alter_column changes to column definitions."""
    return _COLUMN_TO_ALTER[details["column"] in columns](columns, details)


@helper
def _apply_drop_column(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Remove column from schema."""
    dropped = details["column"]
    return {name: col_def for name, col_def in columns.items() if name != dropped}


@helper
def _apply_add_foreign_key(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Add FK reference to column definition."""
    column_name = details["column"]

    # ON DELETE and ON UPDATE are carried only where the operation names one.
    added: dict[str, Any] = {
        "references": details["references"],
        **{
            key: details[key]
            for key in ("on_delete", "on_update")
            if details.get(key)
        },
    }
    return {
        name: {**col_def, **added} if name == column_name else col_def
        for name, col_def in columns.items()
    }


@helper
def _without_the_foreign_key(col_def: Column) -> Column:
    """The same column, with nothing left pointing at another table."""
    return {  # type: ignore[return-value]
        key: value
        for key, value in col_def.items()
        if key not in _FOREIGN_KEY_KEYS
    }


@helper
def _apply_drop_foreign_key(columns: dict[str, Column], details: dict[str, Any]) -> dict[str, Column]:
    """Remove FK reference from column definition."""
    column_name = details["column"]
    return {
        name: _without_the_foreign_key(col_def) if name == column_name else col_def
        for name, col_def in columns.items()
    }


# WHICH OPERATIONS REWRITE A COLUMN DEFINITION. Read by subscript, so an
# operation the rebuild does not rewrite columns for is named by the
# comprehension above rather than absorbed by a default.
_OP_HANDLERS: dict[str, Callable[[dict[str, Column], dict[str, Any]], dict[str, Column]]] = {
    "alter_column": _apply_alter_column,
    "drop_column": _apply_drop_column,
    "add_foreign_key": _apply_add_foreign_key,
    "drop_foreign_key": _apply_drop_foreign_key,
}


# =============================================================================
# Async Execution (SQLite, Turso)
# =============================================================================


@boundary
async def _read_the_foreign_key_pragma(connection: Any) -> int:
    """Whether this connection is enforcing foreign keys right now."""
    fk_cursor = await connection.execute("PRAGMA foreign_keys")
    fk_row = await fk_cursor.fetchone()
    return fk_row[0] if fk_row else 0


@helper
async def _the_caller_owns_the_pragma(_connection: Any) -> int:
    """The caller manages enforcement, so the rebuild reads no state and has
    none to put back afterwards."""
    return 0


# WHO OWNS `PRAGMA foreign_keys` FOR THIS REBUILD. Inside an explicit
# transaction the pragma is a no-op, so a rebuild that set it would report a
# state it had not reached; the caller says which of the two is happening.
_FOREIGN_KEY_STATE: dict[bool, Callable[[Any], Awaitable[int]]] = {
    True: _read_the_foreign_key_pragma,
    False: _the_caller_owns_the_pragma,
}


@boundary
async def _stop_enforcing_foreign_keys(connection: Any) -> bool:
    """Turn enforcement off for the rebuild, which turns it back on after."""
    await connection.execute("PRAGMA foreign_keys = OFF")
    return True


@helper
async def _leave_enforcement_alone(_connection: Any) -> bool:
    """The caller manages enforcement, so the rebuild does not touch it."""
    return False


_FOREIGN_KEYS_OFF: dict[bool, Callable[[Any], Awaitable[bool]]] = {
    True: _stop_enforcing_foreign_keys,
    False: _leave_enforcement_alone,
}




@boundary
async def _say_no_column_is_shared(
    _connection: Any,
    table_name: str,
    _temp_table: str,
    _pairs: list[tuple[str, str]],
) -> int:
    """Nothing in the old table has a home in the new one."""
    logger.warning(
        f"No common columns between old and new schema for '{table_name}'. "
        f"No data will be copied."
    )
    return 0


_WHEN_COLUMNS_ARE_SHARED: dict[
    bool, Callable[[Any, str, str, list[tuple[str, str]]], Awaitable[int]]
] = {
    True: copy_the_shared_columns,
    False: _say_no_column_is_shared,
}


@boundary
async def _copy_the_rows(
    connection: Any,
    table_name: str,
    temp_table: str,
    current_columns: list[str],
    new_columns: list[str],
) -> int:
    """Carry the data across, for the columns both schemas have."""
    # ONE DEFINITION, in `table_reconstruction`. Both modules made the
    # same case-sensitive intersection and both lost the same data.
    from declaro_persistum.abstractions.table_reconstruction import (
        columns_to_copy,
    )

    pairs = columns_to_copy(current_columns, new_columns)
    return await _WHEN_COLUMNS_ARE_SHARED[bool(pairs)](
        connection, table_name, temp_table, pairs
    )


@helper
async def _copy_no_rows(
    _connection: Any,
    _table_name: str,
    _temp_table: str,
    _current_columns: list[str],
    _new_columns: list[str],
) -> int:
    """The caller asked for the schema without the data, so nothing is read."""
    return 0


_WHEN_PRESERVING_DATA: dict[
    bool, Callable[[Any, str, str, list[str], list[str]], Awaitable[int]]
] = {
    True: _copy_the_rows,
    False: _copy_no_rows,
}




@helper
def _no_violations(violations: list[Any]) -> list[Any]:
    """Every foreign key still points at a row that exists."""
    return violations


_WHEN_VIOLATED: dict[bool, Callable[[list[Any]], list[Any]]] = {
    True: refuse_the_violations,
    False: _no_violations,
}


@boundary
async def _enforce_foreign_keys_again(connection: Any) -> list[Any]:
    """Turn enforcement back on and read what it now finds wrong."""
    await connection.execute("PRAGMA foreign_keys = ON")

    # Verify foreign key constraints (all tables — catches incoming FK
    # references from other tables that may now be dangling)
    check_cursor = await connection.execute("PRAGMA foreign_key_check")
    violations = await check_cursor.fetchall()
    return _WHEN_VIOLATED[bool(violations)](violations)


@helper
async def _leave_enforcement_as_it_was(_connection: Any) -> list[Any]:
    """Enforcement was off before the rebuild, or the caller manages it, so
    turning it on here would leave the connection in a state nobody asked for
    and check constraints nobody was checking."""
    return []


_ENFORCEMENT_AFTERWARDS: dict[bool, Callable[[Any], Awaitable[list[Any]]]] = {
    True: _enforce_foreign_keys_again,
    False: _leave_enforcement_as_it_was,
}


@boundary
async def _restore_enforcement_after_failure(connection: Any, table_name: str) -> bool:
    """Put enforcement back before the failure is re-raised."""
    try:
        await connection.execute("PRAGMA foreign_keys = ON")
        return True
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
        return False


@helper
async def _nothing_to_restore(_connection: Any, _table_name: str) -> bool:
    """Enforcement was off before the rebuild, or the caller manages it."""
    return False


_RESTORE_ENFORCEMENT: dict[bool, Callable[[Any, str], Awaitable[bool]]] = {
    True: _restore_enforcement_after_failure,
    False: _nothing_to_restore,
}


@boundary
async def execute_reconstruction_async(
    connection: Any,
    table_name: str,
    new_columns: dict[str, Column],
    *,
    preserve_data: bool | Nothing,
    manage_foreign_keys: bool | Nothing,
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
        >>> await execute_reconstruction_async(conn, "users", new_cols, preserve_data=NOTHING, manage_foreign_keys=NOTHING)
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    preserve_data = resolved(preserve_data, True)
    manage_foreign_keys = resolved(manage_foreign_keys, True)
    # Validate column definitions before any destructive operations
    _validate_columns(table_name, new_columns)

    # One definition, in migrations.py, shared with the recovery that reads
    # these names back. It was a literal here and a literal there, and a change
    # to either is silent: reconstruction keeps working, recovery keeps finding
    # nothing, and a table vanishes after a crash.
    from declaro_persistum.migrations import temp_table_name

    temp_table = temp_table_name(table_name)

    # 1. Fresh introspection - get current state
    current_columns = await table_column_names(connection, table_name)

    # 2. Get current foreign key state
    managed = bool(manage_foreign_keys)
    fk_enabled = await _FOREIGN_KEY_STATE[managed](connection)

    try:
        # 3. Disable foreign keys during reconstruction
        await _FOREIGN_KEYS_OFF[managed](connection)

        # 4. Drop any leftover _new table (and its sqlite_autoindex_* entries)
        #    from a previous failed reconstruction or orphaned recovery.
        await connection.execute(f'DROP TABLE IF EXISTS "{temp_table}"')

        # 5. Create new table with updated schema
        # THE SECOND RECONSTRUCTION MODULE, AND THE FIX LANDED ON THE FIRST.
        # Turso rebuilds through here and SQLite through
        # `table_reconstruction`, so teaching one of them to carry the
        # table-level constraints left the other still dropping them. Fourth
        # time in this session a fix has landed on one copy of a pair.
        from declaro_persistum.abstractions.table_reconstruction import (
            table_constraints_of,
        )

        table_constraints = await table_constraints_of(connection, table_name)
        create_sql = generate_create_table_sql(
            temp_table, new_columns, table_constraints
        )
        logger.debug(f"Creating temp table: {create_sql}")
        await connection.execute(create_sql)

        # 5. Copy compatible data if requested
        await _WHEN_PRESERVING_DATA[bool(preserve_data)](
            connection,
            table_name,
            temp_table,
            list(current_columns),
            list(new_columns),
        )

        # 6. Get indexes to recreate
        indexes = await create_index_statements(connection, table_name)

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

        # 10. Re-enable foreign keys if we managed them, and only if they were
        #     on when the rebuild started.
        await _ENFORCEMENT_AFTERWARDS[managed and bool(fk_enabled)](connection)

        logger.info(f"Successfully reconstructed table '{table_name}'")

    except Exception as e:
        logger.error(f"Table reconstruction failed for '{table_name}': {e}")
        # Re-enable foreign keys before re-raising
        await _RESTORE_ENFORCEMENT[managed and bool(fk_enabled)](
            connection, table_name
        )
        raise


# =============================================================================
# Validation (fail-fast before destructive operations)
# =============================================================================


@helper
def _validate_columns(table_name: str, columns: dict[str, Column]) -> None:
    """Refuse an unwritable column definition before anything destructive runs.

    The refusal itself lives beside the type mappers in `applier.shared`, so
    the other rebuilder gets the same answer. This checked only for a dict,
    and a type written as a number, a list or nothing at all went straight
    into `.lower()` and died naming no column.
    """
    refuse_unwritable_column_types(table_name, columns)


    # RENDER IT BEFORE THE DATABASE IS TOUCHED, so a column definition that
    # cannot be written is refused while the table is still intact rather than
    # halfway through a rebuild.
    #
    # NARROWED FROM `except Exception`, which caught anything at all including
    # a defect in the generator itself and reported it as the caller's invalid
    # column. These three are what a bad definition produces: a missing key, a
    # value of the wrong type, and a value of the right type that is not
    # legal. Anything else is ours and belongs in the traceback.
    try:  # honest: ignore HC-P002: names the bad column before the rebuild
        generate_create_table_sql(f"{table_name}_validate", columns, constraints=NOTHING)
    except (KeyError, TypeError, ValueError) as unrenderable:
        raise ValueError(
            f"Cannot reconstruct table '{table_name}': "
            f"invalid column definitions - {unrenderable}"
        ) from unrenderable


# =============================================================================
# Internal Helper Functions
# =============================================================================






@helper
def _map_type(type_str: str) -> str:
    """The SQLite spelling of a declared type.

    THE NON-STRING FALLBACK IS GONE. This returned "TEXT" for anything that
    was not a string, and `table_reconstruction` raises on the same input, so
    which builder a caller reached decided whether they were told. The
    docstring here recorded that disagreement rather than resolving it.

    TEXT IS A GUESS, AND A REBUILD IS WHERE A GUESS COSTS MOST.
    Reconstruction drops the real table and recreates it from these
    definitions, so a column silently typed TEXT holds text afterwards and
    the original is gone. Raising loses nothing: the table is still there.

    The guard existed because the differ's {"from": x, "to": y} record used to
    reach the renderer unwrapped. `changed_to` resolves that in one place now
    and `_validate_columns` refuses a dict before the database is touched, so
    what is left is a caller who passed something unreadable.
    """
    return sqlite_type(type_str)
