"""Shared pure SQL generation. No I/O, no side effects, no state.

ONE COLUMN VOCABULARY, ONE SPELLING PER ENGINE. A schema declares what a column
means -- `uuid`, `timestamptz`, `numeric(10,2)`, a reference with a cascade --
and each dialect decides how to write it. That translation is `TYPE_MAPS` and
it is the whole multi-dialect surface.

It used to be half a surface. `map_type` took no dialect: it was the SQLite
affinity mapper wearing a neutral name, and `applier/postgresql.py` did not
import it, keeping a second column renderer that differed in exactly one line.
Three byte-identical copies of the SQLite mapper also existed, in
`abstractions/reconstruction.py` and `abstractions/table_reconstruction.py`.
"""

# Re-exported: inspector.shared is the single source of truth for PRAGMA parsing.
import logging
from collections.abc import Callable
from typing import Any

from declaro_persistum.applier.rendering import (
    statements_for as render_statements,
)

# WHAT A VALUE IS SPELLED AS, re-exported so the split changed no
# caller. `spelling.py` holds the other half of what this file used to
# be: the schema says what a column means and that says how each
# engine writes it.
from declaro_persistum.applier.spelling import (
    DEFAULTS as DEFAULTS,
)
from declaro_persistum.applier.spelling import (
    EXPRESSION_DEFAULTS as EXPRESSION_DEFAULTS,
)
from declaro_persistum.applier.spelling import (
    NO_EQUIVALENT as NO_EQUIVALENT,
)
from declaro_persistum.applier.spelling import (
    TYPE_MAPS as TYPE_MAPS,
)
from declaro_persistum.applier.spelling import (
    RenderDefault as RenderDefault,
)
from declaro_persistum.applier.spelling import (
    TypeMap as TypeMap,
)
from declaro_persistum.applier.spelling import (
    column_definition as column_definition,
)
from declaro_persistum.applier.spelling import (
    map_type as map_type,
)
from declaro_persistum.applier.spelling import (
    postgresql_default as postgresql_default,
)
from declaro_persistum.applier.spelling import (
    postgresql_type as postgresql_type,
)
from declaro_persistum.applier.spelling import (
    same_type as same_type,
)
from declaro_persistum.applier.spelling import (
    sqlite_default as sqlite_default,
)
from declaro_persistum.applier.spelling import (
    sqlite_type as sqlite_type,
)
from declaro_persistum.boundary import boundary, helper, recognizer
from declaro_persistum.inspector.shared import (
    columns_from_pragma_rows as columns_from_pragma_rows,
)
from declaro_persistum.reporting import say
from declaro_persistum.types import (
    ApplyResult,
    Column,
    Operation,
    changed_to,
    type_of,
)

# =============================================================================
# DDL generators — all take (table, details) and return SQL string
# =============================================================================


# WHAT EACH CONSTRAINT TYPE IS WRITTEN AS. Two entries, read by subscript.


# AN UNDERSCORE MEANS "THE TABLE REQUIRES IT, THIS ONE DOES NOT READ IT".
#
# Every generator below is an entry in `_SQL_GENERATORS`, so every one takes
# `(table, details)` whether it needs both or not. That is dict-lookup
# polymorphism working (Rule 1): the uniform signature is what lets the
# dispatch be a lookup instead of an if/elif chain.
#
# It is worth marking rather than leaving bare. An argument that is silently
# unread reads the same as one that was forgotten, and this package has just
# been bitten by exactly that: `dialect` was threaded through the whole
# condition renderer, read by nothing, and carrying the literal "sql" from
# `builder.py` for as long as it existed. The convention is already used by
# `_render_order_by(term, _dialect, _path)` in query/expressions.py.


# THE APPLIER RECONSTRUCTS THESE THREE AND THE RENDERER RAISED ON THEM, SO
# THE MIGRATION RAN AND COULD NOT BE PREVIEWED. `apply()` checks
# `requires_reconstruction` before it reaches this table, rebuilds the table,
# and reports success. `generate_sql()` has no such check and came straight
# here, so `declaro generate` died with `NotImplementedError: CODE NOT
# WRITTEN` on a schema change that `declaro apply` performs without complaint.
# Measured 2026-08-18 on SQLite and on Turso, for add_foreign_key,
# drop_foreign_key and alter_column alike.
#
# The comment below is what `dry_run_preview` in this same module has always
# returned for these operations. Two preview paths asked the same question and
# only one of them knew the answer.
#
# It is a comment and not the DDL because the real statements cannot be
# written without a connection: reconstruction reads the table's stored schema
# to know which columns to copy, and `generate_sql` is a pure function that is
# handed operations and nothing else.


# =============================================================================
# Operation dispatch
# =============================================================================

# Maps operation type to pure SQL generator function


@helper
def statements_for(operation: Operation) -> list[str]:
    """The statements an operation runs on SQLite, already separated.

    THE RENDERING LIVES IN ONE PLACE NOW. This was a table of sixteen
    generator functions, and PostgreSQL kept a second table of sixteen more.
    Two tables meant two spellings of the same statement, and a fix reached
    one copy and not the other five separate times. `applier/templates.py`
    holds the text and `applier/rendering.py` fills it, for every engine.
    """
    return render_statements(operation, "sqlite")


@helper
def generate_operation_sql(operation: Operation) -> str:
    """The same statements as one block, for a person to read."""
    return ";\n".join(statements_for(operation))


# `generate_sql` WAS HERE AND IS NOW ONLY IN `applier/__init__.py`. This copy
# took no dialect and rendered through `generate_operation_sql` below, which
# is the SQLite and Turso renderer, so a caller who imported it and passed
# PostgreSQL operations got SQLite SQL and no error. `_alter_column_sql`
# differs entirely between the two: PostgreSQL emits ALTER TABLE ALTER COLUMN,
# this module emits a reconstruction comment.
#
# Nothing called it and coverage reported it dark. The surviving function's
# docstring records what a copy cost last time: the loop lived in
# `applier/postgresql.py` alone, so `declaro generate` worked on PostgreSQL
# and raised AttributeError on the two backends a developer is likeliest to
# be running. It was moved up and given a dispatch table so the copies could
# not drift, and this one was left behind. Guarded by
# `test_no_second_generate_sql_without_a_dialect.py`.


# =============================================================================
# Reconstruction helpers
# =============================================================================

_RECONSTRUCTION_OPS = frozenset(("add_foreign_key", "drop_foreign_key", "alter_column"))


@recognizer
def requires_reconstruction(operation: Operation) -> bool:
    """Check if operation requires table reconstruction."""
    return operation["op"] in _RECONSTRUCTION_OPS


@helper
def _rebuild_note(operation: Operation) -> str:
    """A rebuild reads the stored schema, which a pure renderer cannot do."""
    return f"-- Table reconstruction required for {operation['table']}"


# WHETHER THE OPERATION IS A REBUILD, as a table rather than a branch.
_PREVIEW_LINE: dict[bool, Callable[[Operation], str]] = {
    True: _rebuild_note,
    False: generate_operation_sql,
}


@helper
def dry_run_preview(operations: list[Operation], execution_order: list[int]) -> ApplyResult:
    """Generate dry-run preview of operations."""
    sql_statements = [
        _PREVIEW_LINE[requires_reconstruction(operations[op_idx])](operations[op_idx])
        for op_idx in execution_order
    ]

    return {
        "success": True,
        "executed_sql": sql_statements,
        "operations_applied": len(sql_statements),
        "error": None,
        "error_operation": None,
    }


# Properties a declaration can simply stop mentioning. Everything else the
# differ compares can only change value, never be removed.
REMOVABLE_PROPERTIES = frozenset(("default", "unique", "check"))


@helper
def _refuse_absent_column(
    _columns: dict[str, Column], column: str, operation: Operation
) -> dict[str, Column]:
    """The table has no such column, so the operation names one nobody declared."""
    raise ValueError(f"Column '{column}' not found in table '{operation['table']}'")


@helper
def _the_named_column(
    columns: dict[str, Column], _column: str, _operation: Operation
) -> dict[str, Column]:
    """The table has the column, so the definitions come back unchanged."""
    return columns


_WHEN_ABSENT: dict[bool, Callable[[dict[str, Column], str, Operation], dict[str, Column]]] = {
    True: _refuse_absent_column,
    False: _the_named_column,
}


@helper
def _require_column(
    columns: dict[str, Column], column: str, operation: Operation
) -> dict[str, Column]:
    """Refuse an operation naming a column the table does not have.

    Hands back what it checked, so the answer is a value rather than the
    absence of an exception.
    """
    return _WHEN_ABSENT[column not in columns](columns, column, operation)


@helper
def _reconstruct_alter_column(
    columns: dict[str, Column], operation: Operation
) -> dict[str, Column]:
    """Apply one column's property changes."""
    details = operation["details"]
    column = details["column"]
    _require_column(columns, column, operation)

    for key, raw in details["changes"].items():
        value = changed_to(raw)

        # A `to: None` MEANS THE PROPERTY IS GONE. Assigning it instead put
        # None into the column definition, and the reconstruction renderer
        # tests for the KEY rather than the value, so a CHECK removed from
        # the models became `CHECK (None)` and SQLite read None as a column
        # name. Only the three properties the differ can remove are popped;
        # for anything else a None is a defect to surface.
        removing = value is None and key in REMOVABLE_PROPERTIES
        _WRITE_PROPERTY[removing](columns[column], key, value)

    return columns


@helper
def _reconstruct_add_foreign_key(
    columns: dict[str, Column], operation: Operation
) -> dict[str, Column]:
    """Point a column at another table's column."""
    details = operation["details"]
    column = details["column"]
    _require_column(columns, column, operation)

    # ONE SHAPE. This also accepted {"table": ..., "column": ...}, and
    # measured 2026-08-21 the differ emits "p.id", every producer in src
    # builds that string, and nothing in src, tests or examples writes the
    # dict. Two documents described it, so it was a documented shape that had
    # never been executed and no test had ever covered. An unreached branch is
    # a claim, not support.
    columns[column]["references"] = details["references"]

    for action in ("on_delete", "on_update"):
        columns[column].update({action: details[action]} if action in details else {})

    return columns


@helper
def _reconstruct_drop_foreign_key(
    columns: dict[str, Column], operation: Operation
) -> dict[str, Column]:
    """Remove the reference and the two actions that travel with it."""
    column = operation["details"]["column"]
    _require_column(columns, column, operation)

    for key in ("references", "on_delete", "on_update"):
        columns[column].pop(key, None)  # type: ignore[misc]

    return columns


# WHETHER A PROPERTY IS BEING REMOVED OR SET, as a table rather than a branch.
_WRITE_PROPERTY: dict[bool, Any] = {
    True: lambda column, key, _value: column.pop(key, None),
    False: lambda column, key, value: column.__setitem__(key, value),
}

# WHICH OPERATIONS A REBUILD CAN CARRY. This was an if/elif chain, so an
# operation added to the differ needed a branch here and one removed left a
# branch nothing could reach. Read by subscript: an operation absent from this
# table cannot be reconstructed, and the KeyError says which.
_RECONSTRUCT: dict[str, Any] = {
    "alter_column": _reconstruct_alter_column,
    "add_foreign_key": _reconstruct_add_foreign_key,
    "drop_foreign_key": _reconstruct_drop_foreign_key,
}


@helper
def _the_rebuilder(op_type: str) -> Any:
    """An operation a rebuild carries, so the table hands back its rebuilder."""
    return _RECONSTRUCT[op_type]


@helper
def _refuse_unrebuildable(op_type: str) -> Any:
    """An operation no rebuild carries, named so the caller can see which."""
    raise ValueError(f"Operation '{op_type}' does not support reconstruction")


_CAN_REBUILD: dict[bool, Callable[[str], Any]] = {
    True: _the_rebuilder,
    False: _refuse_unrebuildable,
}


@helper
def apply_reconstruction_changes(
    columns: dict[str, Column], operation: Operation
) -> dict[str, Column]:
    """Apply one reconstruction operation to a table's columns (pure).

    Raises ValueError for an operation a rebuild cannot carry.
    """
    op_type = operation["op"]
    reconstruct = _CAN_REBUILD[op_type in _RECONSTRUCT](op_type)
    return reconstruct(columns, operation)


# The three properties SQLite can change with a single ALTER. Everything else
# the differ compares takes a full rebuild.
_SPECIALISED_ALTERS = frozenset(("nullable", "type", "default"))


@recognizer
def single_change_property(operation: Operation) -> tuple[str, Any] | None:
    """Extract single-property change key and unwrapped value from alter_column op.

    Returns (change_key, value) if the operation is an alter_column with exactly
    one change in {"nullable", "type", "default"}. Returns None otherwise.

    THREE CONDITIONS READ AS THREE FILTERS. The first source list is empty for
    any other operation, so `details["changes"]` is never read for one that
    does not carry it.
    """
    changes = [
        operation["details"]["changes"] for _ in [operation] if operation["op"] == "alter_column"
    ]
    single = [
        (key, changed_to(value))
        for one in changes
        if len(one) == 1
        for key, value in one.items()
        if key in _SPECIALISED_ALTERS
    ]
    return [*single, None][0]


# `enum_population_sql(operation, target_schema)` stood here. It was called
# from one place, `turso.apply`, which received `target_schema` as a keyword
# argument that NO CALLER EVER PASSED, so it was `None` on every run and the
# function's first line returned `[]`. Measured 2026-08-17: the lookup table
# was created empty and rejected every value its column declared.
#
# It is gone rather than wired up, because passing the schema beside the
# operations makes one operation's meaning depend on an argument the others
# ignore. `insert_enum_values` carries its own values and every applier runs
# it the way it runs everything else.


# =============================================================================
# A column type that cannot be written
# =============================================================================

# A COLUMN TYPE IS WRITTEN AS TEXT AND NOTHING ELSE. `sqlite_type` and every
# other mapper lower-cases what it is given, so a type that is not a string
# reaches `.lower()` and dies with an AttributeError naming nothing. The
# author of the broken schema needs to be told which column.
_TYPE_SPELLINGS = frozenset({str})


@helper
def _refuse_unwritable_types(table_name: str, wrong: dict[str, str]) -> dict[str, str]:
    """Columns whose declared type is not text, named so the author finds them.

    A dict here is the differ's `{"from": old, "to": new}` record reaching the
    renderer unwrapped, which `changed_to` resolves in one place now. The
    other spellings -- a number, a list, nothing at all -- were never checked,
    so they crashed inside a type mapper instead of being refused here.
    """
    written = ", ".join(f"'{name}' ({shape})" for name, shape in sorted(wrong.items()))
    raise ValueError(
        f"Table '{table_name}' declares {written}, which is not a column type. "
        f"A type is written as text. A dict means differ output reached the "
        f"renderer unwrapped, so the 'to' value of "
        f"{{'from': ..., 'to': ...}} was never taken."
    )


@helper
def _the_writable_types(_table_name: str, wrong: dict[str, str]) -> dict[str, str]:
    """Every column declares its type as text, so nothing is wrong."""
    return wrong


_WHEN_UNWRITABLE: dict[bool, Callable[[str, dict[str, str]], dict[str, str]]] = {
    True: _refuse_unwritable_types,
    False: _the_writable_types,
}


@helper
def refuse_unwritable_column_types(table_name: str, columns: dict[str, Column]) -> None:
    """Refuse a column whose type cannot be written, before the table is dropped.

    BOTH REBUILDERS CALL THIS. `reconstruction` checked only for a dict and
    `table_reconstruction` checked nothing, so which rebuilder a caller
    reached decided whether they were told what was wrong. A rebuild drops the
    real table and recreates it from these definitions, so a definition that
    cannot be rendered has to be refused while the table is still there.
    """
    wrong = {
        col_name: type_of(col_def["type"]).__name__
        for col_name, col_def in columns.items()
        if type_of(col_def["type"]) not in _TYPE_SPELLINGS
    }
    _WHEN_UNWRITABLE[bool(wrong)](table_name, wrong)


logger = logging.getLogger(__name__)


# =============================================================================
# What both rebuilders do
# =============================================================================

# TWO REBUILDERS SHARE ONE VOCABULARY, and `refuse_unwritable_column_types`
# above is here for the same reason. When both were rewritten as dispatch
# tables on 2026-08-22 they each wrote these two bodies again, because a
# question asked in two places gets answered in two places unless the answer
# has one home.


@boundary
async def copy_the_shared_columns(
    connection: Any,
    table_name: str,
    temp_table: str,
    pairs: list[tuple[str, str]],
) -> int:
    """Move the rows across, reading old names and writing new ones."""
    read_sql = ", ".join(f'"{old}"' for old, _ in pairs)
    write_sql = ", ".join(f'"{new}"' for _, new in pairs)
    copy_sql = f'INSERT INTO "{temp_table}" ({write_sql}) SELECT {read_sql} FROM "{table_name}"'
    say(__name__, "debug", f"Copying data: {copy_sql}")
    await connection.execute(copy_sql)
    return len(pairs)


@helper
def refuse_the_violations(violations: list[Any]) -> list[Any]:
    """The rebuilt table breaks a foreign key, so the caller is told."""
    raise ValueError(f"Foreign key constraint violations after reconstruction: {violations}")
