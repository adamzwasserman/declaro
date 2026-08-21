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
from types import MappingProxyType
from typing import Any

# Re-exported: inspector.shared is the single source of truth for PRAGMA parsing.
from declaro_persistum.inspector.shared import (
    columns_from_pragma_rows as columns_from_pragma_rows,
)
from declaro_persistum.types import ApplyResult, Column, Dialect, Operation, changed_to

# =============================================================================
# Type mapping
# =============================================================================


TypeMap = Callable[[str], str]


def sqlite_type(type_str: str) -> str:
    """SQLite storage classes. Turso shares them.

    Five classes, so uuid, every timestamp and every json flavour all land in
    TEXT. A type the mapper does not recognise is passed through upper-cased
    rather than guessed at.
    """
    type_lower = type_str.lower()

    if "int" in type_lower:
        return "INTEGER"
    elif type_lower in ("text", "varchar", "char", "string") or type_lower.startswith(
        "varchar("
    ):
        return "TEXT"
    elif type_lower in ("boolean", "bool"):
        return "INTEGER"
    elif type_lower in (
        "uuid",
        "timestamptz",
        "timestamp",
        "date",
        "datetime",
    ) or type_lower in ("jsonb", "json"):
        return "TEXT"
    elif type_lower in ("float", "double", "real", "float4", "float8") or type_lower.startswith(
        "numeric"
    ):
        return "REAL"
    elif type_lower in ("blob", "bytea"):
        return "BLOB"
    else:
        return type_str.upper()


def postgresql_type(type_str: str) -> str:
    """PostgreSQL has a real type for everything the vocabulary declares.

    uuid is uuid, timestamptz is timestamptz, numeric(10,2) keeps its precision.
    Nothing to translate, which is exactly why this being missing was invisible:
    the identity function and no function look the same until another engine
    asks.
    """
    return type_str


TYPE_MAPS: dict[str, TypeMap] = {
    "sqlite": sqlite_type,
    "turso": sqlite_type,
    "postgresql": postgresql_type,
}


def same_type(left: str | None, right: str | None, dialect: Dialect) -> bool:
    """Do two declared types name the same column ON THIS ENGINE?

    `uuid` and `text` are one column on SQLite and two on PostgreSQL, so the
    question cannot be answered without the dialect and a string comparison
    cannot answer it at all. The differ compared the raw strings and therefore
    proposed a type change on every migration after the first: introspection
    reports what the engine stored, the models file says what was declared,
    and on SQLite those are never the same word.

    Absent on either side means the caller has no type to compare, which is
    not the same as the types differing; two absences are equal and one is not.
    """
    if left is None or right is None:
        return left is right
    return map_type(left, dialect) == map_type(right, dialect)


def map_type(type_str: str, dialect: Dialect) -> str:
    """Spell a declared type for one engine.

    `dialect` is required (Rule 14). A default would have meant PostgreSQL
    silently receiving SQLite affinity, which is the defect this replaced.
    """
    mapper = TYPE_MAPS.get(dialect)
    if mapper is None:
        raise ValueError(
            f"cannot spell types for dialect {dialect!r}. "
            f"Supported: {sorted(TYPE_MAPS)}."
        )
    return mapper(type_str)


# =============================================================================
# Column definition
# =============================================================================


# How each engine spells the expression defaults a portable schema may declare.
#
# A DEFAULT USED TO BE PASSED THROUGH VERBATIM, so a schema written for one
# engine emitted text another cannot parse. Measured 2026-08-17, running
# `declaro generate` against the schema this package ships:
#
#     "created_at" TEXT DEFAULT now()
#     sqlite3.OperationalError: near "(": syntax error
#
# and the command exited 0. The renderer wrapped parentheses and otherwise
# copied the string, which is a translation table with no entries.
#
# The keys are lowercased and stripped of spaces before lookup, so `NOW()` and
# `now ()` find the same row.
EXPRESSION_DEFAULTS: dict[str, dict[str, str]] = {
    # NO BOOLEAN ROWS. They were here for one day and broke the round trip.
    # Rewriting `false` as `0` meant the schema declared "false", the table was
    # written `DEFAULT 0`, PRAGMA read back "0", and the differ emitted an
    # alter_column on every deploy for a column nobody had touched.
    #
    # Measured on SQLite 3.50.4 and pyturso: both take the keyword.
    #
    #     DEFAULT false -> parses, PRAGMA returns 'false'   (sqlite)
    #     DEFAULT false -> parses, PRAGMA returns 'FALSE'   (turso)
    #
    # Turso uppercases it, which is why `same_default` folds boolean spellings
    # and has since before this table existed. Translating a value the engine
    # already understands turns a working round trip into a migration loop.
    "sqlite": {
        "now()": "(datetime('now'))",
        "current_timestamp": "CURRENT_TIMESTAMP",
    },
    "turso": {
        "now()": "(datetime('now'))",
        "current_timestamp": "CURRENT_TIMESTAMP",
    },
    "postgresql": {},  # PostgreSQL is the language these are written in.
}

# Expressions an engine has NO equivalent for, and what to tell the reader.
#
# THIS IS NOT AN OVERSIGHT IN THE TABLE ABOVE, it is the reason the table has a
# companion. SQLite can produce `lower(hex(randomblob(16)))`, a 32-character
# random hex string that LOOKS like a UUID and is not one -- the version and
# variant bits are wrong, so it is not v4, and anything that parses or
# validates it is being lied to. Translating it would replace a loud syntax
# error with a quiet wrong value.
_NO_UUID = MappingProxyType({
    "gen_random_uuid()": (
        "SQLite has no UUID generator, and a random hex string is not a "
        "v4 UUID. Have the application supply the id"
    ),
    "uuid_generate_v4()": (
        "SQLite has no UUID generator, and a random hex string is not a "
        "v4 UUID. Have the application supply the id"
    ),
})

# WRITTEN ONCE, WHOLE. This was built with sqlite's entry and then mutated on
# the next two lines to add turso and postgresql, so the table a reader sees
# was not the table the code used. Every dialect is here, and the mapping
# cannot be changed after import.
NO_EQUIVALENT = MappingProxyType({
    "sqlite": _NO_UUID,
    "turso": _NO_UUID,
    "postgresql": MappingProxyType({}),
})


def _translate_default(value: Any, dialect: str, column: str) -> Any:
    """The declared default, spelled for `dialect`, or a refusal naming both."""
    if not isinstance(value, str):
        return value

    key = value.strip().lower().replace(" ", "")

    refusal = NO_EQUIVALENT.get(dialect, {}).get(key)
    if refusal is not None:
        raise ValueError(
            f"column {column!r} defaults to {value}, which {dialect} cannot "
            f"spell. {refusal}."
        )

    return EXPRESSION_DEFAULTS.get(dialect, {}).get(key, value)


def sqlite_default(value: Any) -> str:
    """SQLite strips one layer of parens from an expression default.

    PRAGMA table_info returns `now()` for a column declared `DEFAULT (now())`,
    so the round trip loses them and the differ then sees a change that is not
    one. Double-wrapping is what makes introspection agree with the schema.
    """
    if value is None:
        return "DEFAULT NULL"
    if isinstance(value, str) and value.startswith("("):
        return f"DEFAULT ({value})"
    return f"DEFAULT {value}"


def postgresql_default(value: Any) -> str:
    """PostgreSQL returns an expression default as it was written."""
    return "DEFAULT NULL" if value is None else f"DEFAULT {value}"


RenderDefault = Callable[[Any], str]

DEFAULTS: dict[str, RenderDefault] = {
    "sqlite": sqlite_default,
    "turso": sqlite_default,
    "postgresql": postgresql_default,
}


def column_definition(name: str, col: Column, dialect: Dialect) -> str:
    """One column, spelled for one engine.

    There used to be two of these, here and in `applier/postgresql.py`, and
    they differed in two lines: the type and the expression default. Both are
    now looked up by dialect, so the rest -- PRIMARY KEY, NOT NULL, UNIQUE,
    CHECK, REFERENCES with its cascade -- is written once.
    """
    render_default = DEFAULTS.get(dialect)
    if render_default is None:
        raise ValueError(
            f"cannot render a column for dialect {dialect!r}. "
            f"Supported: {sorted(DEFAULTS)}."
        )
    parts = [f'"{name}"', map_type(col.get("type", "text"), dialect)]

    if col.get("primary_key"):
        parts.append("PRIMARY KEY")

    if col.get("nullable") is False:
        parts.append("NOT NULL")

    if col.get("unique"):
        parts.append("UNIQUE")

    if "default" in col:
        parts.append(render_default(_translate_default(col["default"], dialect, name)))

    if "check" in col:
        parts.append(f"CHECK ({col['check']})")

    if "references" in col:
        ref = col["references"]
        ref_table, ref_col = ref.split(".")
        fk_sql = f'REFERENCES "{ref_table}"("{ref_col}")'

        if col.get("on_delete"):
            fk_sql += f" ON DELETE {col['on_delete'].upper().replace('_', ' ')}"
        if col.get("on_update"):
            fk_sql += f" ON UPDATE {col['on_update'].upper().replace('_', ' ')}"

        parts.append(fk_sql)

    return " ".join(parts)


# =============================================================================
# DDL generators — all take (table, details) and return SQL string
# =============================================================================


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


def drop_table_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Generate DROP TABLE statement."""
    return [
        f'DROP TABLE "{table}"'
    ]


def rename_table_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE RENAME statement."""
    new_name = details["new_name"]
    return [
        f'ALTER TABLE "{table}" RENAME TO "{new_name}"'
    ]


def add_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ADD COLUMN statement."""
    col_name = details["column"]
    col_def = details["definition"]
    col_sql = column_definition(col_name, col_def, "sqlite")
    return [
        f'ALTER TABLE "{table}" ADD COLUMN {col_sql}'
    ]


def drop_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate DROP COLUMN statement (SQLite 3.35.0+)."""
    col_name = details["column"]
    return [
        f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'
    ]


def rename_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate RENAME COLUMN statement (SQLite 3.25.0+)."""
    from_col = details["from_column"]
    to_col = details["to_column"]
    return [
        f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'
    ]


def alter_column_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Rendered as the note `apply` acts on, because `apply` does act on it.

    See `_reconstruction_note` below for why this is a comment and not DDL.
    """
    return [
        _reconstruction_note(table, "altering a column")
    ]


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
def _reconstruction_note(table: str, what: str) -> str:
    return f"-- Table reconstruction required for {table} ({what})"


def add_constraint_sql(table: str, details: dict[str, Any]) -> list[str]:
    """SQLite and Turso cannot add a named constraint to an existing table."""
    raise NotImplementedError(
        f"Cannot add constraint {details['constraint']!r} to {table!r}: SQLite "
        f"and Turso have no ADD CONSTRAINT, and this package does not yet "
        f"rebuild a table to add one. Declare the constraint in the model "
        f"before the table is first created, or add it by hand."
    )


def drop_constraint_sql(table: str, details: dict[str, Any]) -> list[str]:
    """SQLite and Turso cannot drop a named constraint from an existing table."""
    raise NotImplementedError(
        f"Cannot drop constraint {details['constraint']!r} from {table!r}: "
        f"SQLite and Turso have no DROP CONSTRAINT, and this package does not "
        f"yet rebuild a table to remove one. Drop it by hand, or recreate the "
        f"table."
    )


def add_foreign_key_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Rendered as the note `apply` acts on, because `apply` does act on it."""
    return [
        _reconstruction_note(table, "adding a foreign key")
    ]


def drop_foreign_key_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Rendered as the note `apply` acts on, because `apply` does act on it."""
    return [
        _reconstruction_note(table, "dropping a foreign key")
    ]


def create_view_sql(_table: str, details: dict[str, Any]) -> list[str]:
    """Generate CREATE VIEW statement."""
    from declaro_persistum.applier.sqlite import generate_create_view

    return generate_create_view(details)  # type: ignore[arg-type]


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


def requires_reconstruction(operation: Operation) -> bool:
    """Check if operation requires table reconstruction."""
    return operation["op"] in _RECONSTRUCTION_OPS


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


def _require_column(
    columns: dict[str, Column], column: str, operation: Operation
) -> None:
    """Refuse an operation naming a column the table does not have."""
    if column not in columns:
        raise ValueError(
            f"Column '{column}' not found in table '{operation['table']}'"
        )


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
