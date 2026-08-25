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
from collections.abc import Callable
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
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.reporting import say
from declaro_persistum.types import NOTHING, Column, Nothing, resolved

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
    preserve_data: bool | Nothing,
    manage_foreign_keys: bool | Nothing,
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
        >>> await reconstruct_table(conn, "users", new_cols, preserve_data=NOTHING, manage_foreign_keys=NOTHING)
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    preserve_data = resolved(preserve_data, True)
    manage_foreign_keys = resolved(manage_foreign_keys, True)
    # One definition, in migrations.py, shared with the recovery that reads
    # these names back. It was a literal here and a literal there, and a change
    # to either is silent: reconstruction keeps working, recovery keeps finding
    # nothing, and a table vanishes after a crash.
    from declaro_persistum.migrations import temp_table_name

    temp_table = temp_table_name(table_name)
    fk_enabled = 0

    try:
        # 1. and 2. Read the foreign key state, then turn enforcement off for
        # the rebuild. A caller that manages the pragma itself gets neither,
        # and enforcement stays reported as off, which is what the rest of
        # this function assumed when it skipped the read.
        state_read = [
            await _disable_foreign_keys(connection)
            for managed in [manage_foreign_keys]
            if managed
        ]
        fk_enabled = [*state_read, 0][0]

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
        say(__name__, "debug", f"Creating temp table: {create_sql}")
        await connection.execute(create_sql)

        # 4. Copy compatible data if requested. The old table is read only
        # when a copy is wanted, and a schema sharing no column with it gets
        # the warning instead of an INSERT naming nothing.
        stored_columns = [
            await table_column_names(connection, table_name)
            for wanted in [preserve_data]
            if wanted
        ]
        shared_columns = [
            columns_to_copy(stored, list(new_columns)) for stored in stored_columns
        ]
        _copied = [
            await copy_the_shared_columns(connection, table_name, temp_table, pairs)
            for pairs in shared_columns
            if pairs
        ]
        _warned = [
            _warn_that_no_column_is_shared(table_name)
            for pairs in shared_columns
            if not pairs
        ]

        # 5. Get indexes and constraints to recreate
        indexes = await create_index_statements(connection, table_name)

        # 6. Drop old table
        await connection.execute(f'DROP TABLE "{table_name}"')

        # 7. Rename temp table to original name
        await connection.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')

        # 8. Recreate indexes (substituting new table name)
        _recreated = [
            await _recreate_index(connection, index_sql, temp_table, table_name)
            for index_sql in indexes
        ]

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
        #
        # Verify foreign key constraints, which is worth doing only where this
        # rebuild is the one that turned enforcement off.
        _checked = [
            await _check_the_foreign_keys(connection, table_name)
            for enforcing in [manage_foreign_keys and fk_enabled]
            if enforcing
        ]

        say(__name__, "info", f"Successfully reconstructed table '{table_name}'")

    except Exception:
        # The exception carries what went wrong, so writing it here as well
        # is a second copy on a channel the caller cannot read.
        # Re-enable foreign keys before re-raising, for the same connections
        # this rebuild turned them off for.
        _restored = [
            await _restore_the_pragma(connection, table_name)
            for enforcing in [manage_foreign_keys and fk_enabled]
            if enforcing
        ]
        raise


@boundary
async def _disable_foreign_keys(connection: Any) -> int:
    """Turn enforcement off for the rebuild, and report what it was.

    The state is read before it is changed, because whoever commits has to
    know what to put back.
    """
    fk_cursor = await connection.execute("PRAGMA foreign_keys")
    fk_row = await fk_cursor.fetchone()
    enabled: int = fk_row[0] if fk_row else 0
    await connection.execute("PRAGMA foreign_keys = OFF")
    return enabled




def _warn_that_no_column_is_shared(table_name: str) -> None:
    """Nothing is in both tables, so the rebuilt table comes out empty.

    An INSERT naming no column is not SQL, so the empty case is a warning
    rather than a statement.
    """
    say(__name__, "warning",
        f"No common columns between old and new schema for '{table_name}'. "
        f"No data will be copied."
    )


@boundary
async def _recreate_index(
    connection: Any, index_sql: str, temp_table: str, table_name: str
) -> str:
    """One index, reissued against the table under its final name."""
    recreate_sql = index_sql.replace(f'"{temp_table}"', f'"{table_name}"')
    say(__name__, "debug", f"Recreating index: {recreate_sql}")
    await connection.execute(recreate_sql)
    return recreate_sql




@boundary
async def _check_the_foreign_keys(connection: Any, table_name: str) -> list[Any]:
    """Read every reference in the rebuilt table back, and refuse a broken one."""
    check_cursor = await connection.execute(f'PRAGMA foreign_key_check("{table_name}")')
    violations = list(await check_cursor.fetchall())
    _refused = [refuse_the_violations(broken) for broken in [violations] if broken]
    return violations


@boundary
async def _restore_the_pragma(connection: Any, table_name: str) -> list[str]:
    """Put enforcement back after a failed rebuild, and say what ran."""
    try:
        await connection.execute("PRAGMA foreign_keys = ON")
        return ["PRAGMA foreign_keys = ON"]
    except Exception as fk_exc:
        # The original failure is about to be re-raised, but this one leaves
        # the connection enforcing nothing. A caller that catches the
        # reconstruction error and carries on would write against unchecked
        # constraints and never know.
        logger.error(
            "Could not re-enable foreign keys after the failed "
            "reconstruction of '%s': %s. THIS CONNECTION NOW HAS "
            "FOREIGN KEY ENFORCEMENT OFF.",
            table_name, fk_exc,
        )
        return []


@helper
def _with_the_property(prop: str) -> Callable[[Column, Any], Column]:
    """A writer that puts one named property onto a column declaration."""
    return lambda col_def, value: {**col_def, prop: value}


# HOW EACH PROPERTY IS WRITTEN ONTO A COLUMN. Setting one is the same edit
# whatever it is called, which is why `alter_column_nullability` and
# `alter_column_type` were the same function with a different word in one
# place. Removing a default is not the same edit: it deletes the key rather
# than setting it to None, because a column with `default: None` and a column
# with no default are different declarations.
_HOW_TO_SET: dict[str, Callable[[Column, Any], Column]] = {
    "nullable": _with_the_property("nullable"),
    "type": _with_the_property("type"),
}


@helper
async def alter_column_property(
    connection: Any,
    table_name: str,
    column_name: str,
    prop: str,
    value: Any,
) -> None:
    """Set one property on one column, and rebuild the table around it.

    SQLite has no ALTER COLUMN, so every change to a column is a rebuild: make
    the table again with the new declaration and copy the rows across. What
    varies between one property and the next is only which key the new
    declaration carries, so that is the argument and not a separate function.
    """
    columns = await _get_full_table_schema(connection, table_name)
    changing = _the_column_to_change(table_name, column_name, columns)
    changed = {**columns, column_name: _HOW_TO_SET[prop](changing, value)}
    await reconstruct_table(
        connection, table_name, changed,
        preserve_data=NOTHING, manage_foreign_keys=NOTHING,
    )


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
    changing = _the_column_to_change(table_name, column_name, columns)

    # Update default. None asks for the default to go, and any other value
    # asks for that value, so the two are separate functions.
    changed = {
        **columns,
        column_name: _WHEN_A_DEFAULT_IS_GIVEN[default_value is not None](
            changing, default_value
        ),
    }

    # Reconstruct table
    await reconstruct_table(connection, table_name, changed, preserve_data=NOTHING, manage_foreign_keys=NOTHING)


@helper
def _with_default(col_def: Column, default_value: str | None) -> Column:
    """The column carrying the default the caller named."""
    return {**col_def, "default": default_value}


@helper
def _without_default(col_def: Column, _default_value: str | None) -> Column:
    """The column with no default at all, which is what None asks for."""
    return {key: value for key, value in col_def.items() if key != "default"}


_WHEN_A_DEFAULT_IS_GIVEN: dict[bool, Callable[[Column, str | None], Column]] = {
    True: _with_default,
    False: _without_default,
}


@helper
def _no_such_column(
    table_name: str, column_name: str, _columns: dict[str, Column]
) -> Column:
    """A column this table does not have, named so the caller finds it.

    BEFORE ANYTHING IS DROPPED. The rebuild these three functions call drops
    the real table, so a column nobody can change has to be refused while the
    table is still there.
    """
    raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")


@helper
def _the_named_column(
    _table_name: str, column_name: str, columns: dict[str, Column]
) -> Column:
    """The definition the table holds for this column."""
    return columns[column_name]


_WHEN_THE_COLUMN_EXISTS: dict[
    bool, Callable[[str, str, dict[str, Column]], Column]
] = {
    True: _the_named_column,
    False: _no_such_column,
}


@helper
def _the_column_to_change(
    table_name: str, column_name: str, columns: dict[str, Column]
) -> Column:
    """The column about to be altered, or a refusal naming it."""
    return _WHEN_THE_COLUMN_EXISTS[column_name in columns](
        table_name, column_name, columns
    )


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
    columns: dict[str, Column] = {row[1]: _column_from_pragma(row) for row in rows}

    # Inline UNIQUE constraints are auto-generated indexes with origin 'u'.
    # An index row is (seq, name, unique, origin, partial).
    index_list = await pragma_index_list(connection, table_name)
    unique_indexes = [
        await _index_columns(connection, idx_row[1])
        for idx_row in index_list
        if idx_row[3] == "u" and idx_row[2]
    ]
    unique_columns = [covered[0][2] for covered in unique_indexes if len(covered) == 1]

    fk_list = await pragma_foreign_key_list(connection, table_name)

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
    checks = [
        (name, expression)
        for row in ddl_rows[:1]
        if row[0]
        for name, expression in checks_from_ddl(row[0]).items()
    ]

    return _with_properties(
        columns,
        [
            *[(name, {"unique": True}) for name in unique_columns],
            *[(fk_row[3], _foreign_key_properties(fk_row)) for fk_row in fk_list],
            *[(name, {"check": expression}) for name, expression in checks],
        ],
    )


@helper
def _column_from_pragma(row: Any) -> Column:
    """One column as PRAGMA table_info reports it.

    The row is (cid, name, type, notnull, dflt_value, pk). A property the row
    does not carry is left out rather than written as a false one.
    """
    type_str, notnull, dflt_value, pk = row[2], row[3], row[4], row[5]
    column: dict[str, Any] = dict(
        [
            ("type", type_str or "TEXT"),
            ("nullable", not bool(notnull)),
            *[("primary_key", True) for is_key in [pk] if is_key],
            *[("default", value) for value in [dflt_value] if value is not None],
        ]
    )
    return column  # type: ignore[return-value]


@boundary
async def _index_columns(connection: Any, index_name: str) -> list[Any]:
    """The columns one index covers, as PRAGMA index_info reports them."""
    cursor = await connection.execute(f'PRAGMA index_info("{index_name}")')
    return list(await cursor.fetchall())


# What SQLite reports for a reference that declared no action. None of the
# three is worth recording, and recording one makes a rebuilt table differ
# from the schema that asked for it.
_NO_ACTION = ("NO ACTION", "NONE", "")


@helper
def _is_a_declared_action(action: Any) -> bool:
    """Whether the engine reported an action somebody actually asked for."""
    return bool(action) and action.upper() not in _NO_ACTION


@helper
def _foreign_key_properties(fk_row: Any) -> dict[str, Any]:
    """The reference one PRAGMA foreign_key_list row declares.

    The row is (id, seq, table, from, to, on_update, on_delete, match).
    """
    ref_table, ref_col = fk_row[2], fk_row[4]
    return dict(
        [
            ("references", f"{ref_table}.{ref_col}"),
            *[
                ("on_delete", action.lower())
                for action in [fk_row[6]]
                if _is_a_declared_action(action)
            ],
            *[
                ("on_update", action.lower())
                for action in [fk_row[5]]
                if _is_a_declared_action(action)
            ],
        ]
    )


@helper
def _with_properties(
    columns: dict[str, Column], extra: list[tuple[str, dict[str, Any]]]
) -> dict[str, Column]:
    """The columns, each carrying the extra properties that name it.

    A property naming a column the table does not have is dropped, which is
    what the three separate `in columns` tests here used to do one at a time.
    """
    return {
        name: {
            **col_def,
            **{
                key: value
                for named, properties in extra
                if named == name
                for key, value in properties.items()
            },
        }
        for name, col_def in columns.items()
    }


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
    # A table nobody stored DDL for carries no constraint, so the comprehension
    # over the nought-or-one row is the whole answer.
    return {
        name: definition
        for row in rows[:1]
        if row[0]
        for name, definition in table_constraints_from_ddl(row[0]).items()
    }




@helper
def _generate_create_table_sql(
    table_name: str,
    columns: dict[str, Column],
    constraints: dict[str, dict[str, str]] | None | Nothing,
) -> str:
    """
    Generate CREATE TABLE statement from column definitions.

    Args:
        table_name: Name of table
        columns: Dict mapping column name to Column definition

    Returns:
        CREATE TABLE SQL statement
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    constraints = resolved(constraints, None)
    # BEFORE ANYTHING IS WRITTEN, so every offending column is named at once
    # rather than the caller fixing them one crash at a time. This rebuilder
    # checked nothing at all, so which of the two a caller reached decided
    # whether they were told what was wrong with their schema.
    refuse_unwritable_column_types(table_name, columns)

    # Written through the shared clause so a rebuilt table cannot spell a
    # constraint differently from the create that first made it.
    from declaro_persistum.applier.spelling import constraint_clause

    pk_columns = [
        col_name for col_name, col_def in columns.items() if col_def.get("primary_key")
    ]
    col_defs: list[str] = [
        # A key over one column is written on that column; a key over several
        # cannot be, so it becomes a clause of its own below.
        *[
            _column_clause(col_name, col_def, [col_name] == pk_columns)
            for col_name, col_def in columns.items()
        ],
        *[_composite_primary_key(keys) for keys in [pk_columns] if len(keys) > 1],
        *[
            constraint_clause(name, definition)
            for name, definition in (constraints or {}).items()
        ],
    ]

    columns_sql = ",\n    ".join(col_defs)
    return f'CREATE TABLE "{table_name}" (\n    {columns_sql}\n)'


@helper
def _column_clause(col_name: str, col_def: Column, inline_primary_key: bool) -> str:
    """One column, as it appears inside a CREATE TABLE.

    Each clause is a comprehension over the one value that decides it, so a
    clause the column does not carry contributes nothing.
    """
    return " ".join(
        [
            f'"{col_name}"',
            # NO DEFAULT ON THE TYPE. A column that never said what it is does
            # not become a TEXT column; the loader refuses one, so reaching
            # here means a caller built the definition by hand and left the
            # type out.
            _map_type(col_def["type"]),
            *["PRIMARY KEY" for alone in [inline_primary_key] if alone],
            *["NOT NULL" for nullable in [col_def.get("nullable")] if nullable is False],
            *["UNIQUE" for unique in [col_def.get("unique")] if unique],
            # ONE RULE WRITES THIS CLAUSE, and it was written twice. This read
            # `f"DEFAULT {col_def['default']}"` while the applier and the other
            # rebuilder both called `sqlite_default`, which wraps an expression
            # a second time because `PRAGMA table_info` strips one layer. So a
            # rebuild through this module returned `(datetime('now'))` a layer
            # shallower each time, the differ saw a change that was not one,
            # and the schema migrated on every deploy. It also formatted a
            # default with no SQL spelling straight into the statement, and
            # this renderer runs after the real table has been dropped.
            *[sqlite_default(col_def[key]) for key in ["default"] if key in col_def],
            *[f"CHECK ({col_def['check']})" for key in ["check"] if key in col_def],
            *[_reference_clause(col_def) for key in ["references"] if key in col_def],
        ]
    )


@helper
def _reference_clause(col_def: Column) -> str:
    """The REFERENCES clause a column carries, with the actions it declares."""
    ref = col_def["references"]
    # A reference naming only a table means that table's `id`.
    ref_table, ref_col = ref.split(".", 1) if "." in ref else (ref, "id")
    return "".join(
        [
            f'REFERENCES "{ref_table}"("{ref_col}")',
            *[
                f" ON DELETE {_written_action(action)}"
                for action in [col_def.get("on_delete")]
                if action
            ],
            *[
                f" ON UPDATE {_written_action(action)}"
                for action in [col_def.get("on_update")]
                if action
            ],
        ]
    )


@helper
def _written_action(action: str) -> str:
    """A declared action as SQL spells it: `set_null` is `SET NULL`."""
    return action.upper().replace("_", " ")


@helper
def _composite_primary_key(pk_columns: list[str]) -> str:
    """A key over more than one column, which no single column can carry."""
    written = ", ".join(f'"{c}"' for c in pk_columns)
    return f"PRIMARY KEY ({written})"


# SQLite spelling. This was a byte-for-byte copy of `applier.shared.sqlite_type`.
# It carries no non-string guard and raises AttributeError on a dict, which is
# how it has always behaved; `abstractions/reconstruction._map_type` returns
# TEXT for that input. The divergence is recorded, not resolved here.
_map_type = sqlite_type
