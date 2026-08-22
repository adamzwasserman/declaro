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

from collections.abc import Callable
from typing import Any

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

# Re-exported: inspector.shared is the single source of truth for PRAGMA parsing.
from declaro_persistum.boundary import helper, recognizer
from declaro_persistum.inspector.shared import (
    columns_from_pragma_rows as columns_from_pragma_rows,
)
from declaro_persistum.types import (
    ApplyResult,
    Column,
    Operation,
    changed_to,
    spelling_of,
)

# =============================================================================
# DDL generators — all take (table, details) and return SQL string
# =============================================================================


@helper
def insert_enum_values_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Fill a `_dp_enum_` lookup table with the values its column declares.

    ONE STATEMENT PER VALUE, RETURNED AS A LIST. This used to join them with
    `;` because the SQLite and Turso appliers split a generated string on `;`.
    A semicolon is not a statement separator, and `Literal["ok", "a;b"]` is
    ordinary Python that the loader accepts, so the split produced two
    fragments that were each invalid SQL:

        INSERT INTO "_dp_enum_status" (value) VALUES ('a
        b')

    THE VALUES ARE QUOTED BY DOUBLING THE APOSTROPHE, which is SQL's own
    escape. They come from a `Literal[...]` in a models file, so they are
    developer-authored rather than user input, but `it's live` is ordinary
    English and would end the string literal early. There is no escape for a
    semicolon because SQL needs none; the caller had invented a reason.
    """
    values = details["values"]
    return [
        f"""INSERT INTO "{table}" (value) VALUES ('{value.replace("'", "''")}')"""
        for value in values
    ]


@helper
def constraint_clause(name: str, definition: dict[str, Any]) -> str:
    """One table-level constraint, as it appears inside a CREATE TABLE.

    Shared by the renderer above and by the reconstruction that rewrites a
    table, so a rebuild cannot spell a constraint differently from the create.
    """
    kind = definition.get("type")
    if kind == "check":
        return f'CONSTRAINT "{name}" CHECK ({definition["expression"]})'
    if kind == "unique":
        columns = ", ".join(f'"{c}"' for c in definition.get("columns", []))
        return f'CONSTRAINT "{name}" UNIQUE ({columns})'
    raise ValueError(
        f"constraint {name!r} has type {kind!r}; this renderer writes "
        f"{['check', 'unique']}."
    )


@helper
def create_table_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate CREATE TABLE statement."""
    columns = details.get("columns", {})
    primary_key = details.get("primary_key", [])

    col_defs: list[str] = []

    for col_name, col_def in columns.items():
        col_sql = column_definition(col_name, col_def, "sqlite")
        col_defs.append(col_sql)

    if primary_key and len(primary_key) > 1:
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

    # A TABLE-LEVEL CONSTRAINT IS CARRIED HERE AND WAS DROPPED ON THE FLOOR.
    # This function read `columns` and `primary_key` and ignored
    # `constraints`, so a declared constraint was never created, the migration
    # reported success, and every deploy afterwards saw it missing and emitted
    # `add_constraint` -- which SQLite and Turso refuse by name, so every
    # deploy after the first failed. Measured 2026-08-19.
    #
    # It is the same defect `add_index` had this morning, in the same
    # function, on the next key along: details carried it and the renderer did
    # not look.
    #
    # THE FIX IS DIFFERENT AND THAT IS THE POINT. An index became a separate
    # `add_index` operation beside the create, because CREATE INDEX is a
    # statement every engine has. A table-level constraint is not: these
    # engines have no ADD CONSTRAINT, so inside the CREATE TABLE is the only
    # moment one can exist.
    #
    # Named, because an unnamed constraint cannot be dropped by name later.
    for name, definition in (details.get("constraints") or {}).items():
        col_defs.append(constraint_clause(name, definition))

    columns_sql = ",\n    ".join(col_defs)
    return [
        f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'
    ]


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


@helper
def drop_table_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Generate DROP TABLE statement."""
    return [
        f'DROP TABLE "{table}"'
    ]


@helper
def rename_table_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE RENAME statement."""
    new_name = details["new_name"]
    return [
        f'ALTER TABLE "{table}" RENAME TO "{new_name}"'
    ]


@helper
def add_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ADD COLUMN statement."""
    col_name = details["column"]
    col_def = details["definition"]
    col_sql = column_definition(col_name, col_def, "sqlite")
    return [
        f'ALTER TABLE "{table}" ADD COLUMN {col_sql}'
    ]


@helper
def drop_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate DROP COLUMN statement (SQLite 3.35.0+)."""
    col_name = details["column"]
    return [
        f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'
    ]


@helper
def rename_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate RENAME COLUMN statement (SQLite 3.25.0+)."""
    from_col = details["from_column"]
    to_col = details["to_column"]
    return [
        f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'
    ]


@helper
def alter_column_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Rendered as the note `apply` acts on, because `apply` does act on it.

    See `_reconstruction_note` below for why this is a comment and not DDL.
    """
    return [
        _reconstruction_note(table, "altering a column")
    ]


@helper
def add_index_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate CREATE INDEX statement."""
    idx_name = details["index"]
    idx_def = details["definition"]
    columns = idx_def.get("columns", [])
    unique = "UNIQUE " if idx_def.get("unique") else ""
    where = f" WHERE {idx_def['where']}" if idx_def.get("where") else ""

    cols_sql = ", ".join(f'"{c}"' for c in columns)
    return [
        f'CREATE {unique}INDEX "{idx_name}" ON "{table}" ({cols_sql}){where}'
    ]


@helper
def drop_index_sql(_table: str, details: dict[str, Any]) -> list[str]:
    """Generate DROP INDEX statement."""
    idx_name = details["index"]
    return [
        f'DROP INDEX "{idx_name}"'
    ]


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
@helper
def _reconstruction_note(table: str, what: str) -> str:
    return f"-- Table reconstruction required for {table} ({what})"


@helper
def add_constraint_sql(table: str, details: dict[str, Any]) -> list[str]:
    """SQLite and Turso cannot add a named constraint to an existing table."""
    raise NotImplementedError(
        f"Cannot add constraint {details['constraint']!r} to {table!r}: SQLite "
        f"and Turso have no ADD CONSTRAINT, and this package does not yet "
        f"rebuild a table to add one. Declare the constraint in the model "
        f"before the table is first created, or add it by hand."
    )


@helper
def drop_constraint_sql(table: str, details: dict[str, Any]) -> list[str]:
    """SQLite and Turso cannot drop a named constraint from an existing table."""
    raise NotImplementedError(
        f"Cannot drop constraint {details['constraint']!r} from {table!r}: "
        f"SQLite and Turso have no DROP CONSTRAINT, and this package does not "
        f"yet rebuild a table to remove one. Drop it by hand, or recreate the "
        f"table."
    )


@helper
def add_foreign_key_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Rendered as the note `apply` acts on, because `apply` does act on it."""
    return [
        _reconstruction_note(table, "adding a foreign key")
    ]


@helper
def drop_foreign_key_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Rendered as the note `apply` acts on, because `apply` does act on it."""
    return [
        _reconstruction_note(table, "dropping a foreign key")
    ]


@helper
def create_view_sql(_table: str, details: dict[str, Any]) -> list[str]:
    """Generate CREATE VIEW statement."""
    from declaro_persistum.applier.sqlite import generate_create_view

    return generate_create_view(details)  # type: ignore[arg-type]


@helper
def drop_view_sql(_table: str, details: dict[str, Any]) -> list[str]:
    """The statements that remove a view on SQLite and Turso.

    RETURNED, NOT WRAPPED. `generate_drop_view` builds the list itself,
    because a materialized view here is a table with a registry row beside it
    and removing it takes more than one statement.
    """
    from declaro_persistum.applier.sqlite import generate_drop_view

    return generate_drop_view(details["name"], bool(details.get("materialized")))


# =============================================================================
# Operation dispatch
# =============================================================================

# Maps operation type to pure SQL generator function
_SQL_GENERATORS: dict[str, Any] = {
    "create_table": create_table_sql,
    # THE SIXTEENTH. A `Literal` column becomes a lookup table plus a foreign
    # key, and nothing filled the lookup table, so every declared value was
    # rejected by the constraint that exists to permit exactly those values.
    "insert_enum_values": insert_enum_values_sql,
    "drop_table": drop_table_sql,
    "rename_table": rename_table_sql,
    "add_column": add_column_sql,
    "drop_column": drop_column_sql,
    "rename_column": rename_column_sql,
    "alter_column": alter_column_sql,
    "add_index": add_index_sql,
    "drop_index": drop_index_sql,
    "add_constraint": add_constraint_sql,
    "drop_constraint": drop_constraint_sql,
    "add_foreign_key": add_foreign_key_sql,
    "drop_foreign_key": drop_foreign_key_sql,
    "create_view": create_view_sql,
    "drop_view": drop_view_sql,
}


@helper
def statements_for(operation: Operation) -> list[str]:
    """The statements an operation runs, already separated.

    THE SPLIT IS GONE BECAUSE THE JOIN IS GONE. The appliers used to take a
    generated string and cut it with `str.split(";")`, which is wrong twice: a
    trigger body carries semicolons between BEGIN and END, and so does any
    string literal holding one. Both arrived in real declarations. A generator
    that needs several statements returns several, and nothing between here
    and the engine has to know what a semicolon means.
    """
    op_type = operation["op"]

    generator = _SQL_GENERATORS.get(op_type)
    if not generator:
        raise ValueError(f"Unknown operation type: {op_type}")

    return generator(operation["table"], operation["details"])


@helper
def generate_operation_sql(operation: Operation) -> str:
    """The same statements as one block, for a person to read.

    JOINING IS SAFE HERE AND WAS NOT SAFE ON THE EXECUTION PATH, because
    nothing parses this back. It is what `--dry-run` prints.
    """
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
def dry_run_preview(
    operations: list[Operation], execution_order: list[int]
) -> ApplyResult:
    """Generate dry-run preview of operations."""
    sql_statements = []
    for op_idx in execution_order:
        operation = operations[op_idx]
        if requires_reconstruction(operation):
            sql_statements.append(
                f"-- Table reconstruction required for {operation['table']}"
            )
        else:
            sql_statements.append(generate_operation_sql(operation))

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
def _require_column(
    columns: dict[str, Column], column: str, operation: Operation
) -> None:
    """Refuse an operation naming a column the table does not have."""
    if column not in columns:
        raise ValueError(
            f"Column '{column}' not found in table '{operation['table']}'"
        )


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
def apply_reconstruction_changes(
    columns: dict[str, Column], operation: Operation
) -> dict[str, Column]:
    """Apply one reconstruction operation to a table's columns (pure).

    Raises KeyError for an operation a rebuild cannot carry.
    """
    op_type = operation["op"]
    reconstruct = _RECONSTRUCT.get(op_type)
    if reconstruct is None:
        raise ValueError(f"Operation '{op_type}' does not support reconstruction")
    return reconstruct(columns, operation)


@recognizer
def single_change_property(operation: Operation) -> tuple[str, Any] | None:
    """Extract single-property change key and unwrapped value from alter_column op.

    Returns (change_key, value) if the operation is an alter_column with exactly
    one change in {"nullable", "type", "default"}. Returns None otherwise.
    """
    if operation["op"] != "alter_column":
        return None

    changes = operation["details"]["changes"]
    if len(changes) != 1:
        return None

    for key in ("nullable", "type", "default"):
        if key in changes:
            val = changes[key]
            val = changed_to(val)
            return key, val

    return None


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
def _refuse_unwritable_types(
    table_name: str, wrong: dict[str, str]
) -> dict[str, str]:
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
        col_name: spelling_of(col_def["type"]).__name__
        for col_name, col_def in columns.items()
        if spelling_of(col_def["type"]) not in _TYPE_SPELLINGS
    }
    _WHEN_UNWRITABLE[bool(wrong)](table_name, wrong)
