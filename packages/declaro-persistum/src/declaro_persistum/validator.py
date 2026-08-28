"""
Schema validation.

Validates schema definitions for correctness:
- Foreign key references exist
- Index columns exist
- No circular dependencies (without deferrable)
- Type names are valid
"""

from collections.abc import Callable
from functools import partial, reduce
from typing import TypedDict

from declaro_persistum.exceptions import ValidationError
from declaro_persistum.types import NOTHING, Column, Index, Schema, Table, is_nullable

# Known SQL types (normalized lowercase)
KNOWN_TYPES = {
    # Standard SQL
    "integer",
    "bigint",
    "smallint",
    "text",
    "varchar",
    "char",
    "boolean",
    "bool",
    "real",
    "float",
    "float4",
    "float8",
    "double precision",
    "numeric",
    "decimal",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "interval",
    "blob",
    "bytea",
    # PostgreSQL specific
    "uuid",
    "jsonb",
    "json",
    "serial",
    "bigserial",
    "smallserial",
    "money",
    "inet",
    "cidr",
    "macaddr",
    "point",
    "line",
    "lseg",
    "box",
    "path",
    "polygon",
    "circle",
    "tsvector",
    "tsquery",
    "xml",
    # Arrays
    "integer[]",
    "text[]",
    "uuid[]",
    "jsonb[]",
}


class ValidationResult(TypedDict):
    """What a validation found, reached by name.

    Both validators returned `tuple[list[str], list[str]]` as
    `(warnings, errors)`. The annotation is honest about the types and silent
    about the only thing a caller needs, and a swapped pair of same-typed
    lists does not raise: it inverts a check and passes.

    Measured on myself, 2026-08-17. Writing a test against that signature I
    subscripted the result as a dict, then unpacked it as `errors, warnings`.
    The second version asserted the absence of warnings by looking at the
    errors, and it passed, because both lists were empty.
    """

    warnings: list[str]
    errors: list[str]


# WHAT THE RENDERERS CANNOT CARRY, and why each one breaks.
#
#     "   the appliers write an identifier as `"{name}"` at 65 places and
#         none escapes it, so `quo"ted` renders `"quo"ted"` and the engine
#         reads three tokens. Both SQLite and PostgreSQL refuse it.
#
#     ;   the SQLite and Turso appliers split rendered SQL on `;` before
#         executing, so a name holding one is torn in half. PostgreSQL runs
#         the statement whole and takes it without complaint.
#
# Measured 2026-08-18 by declaring one table per name and migrating. Refusing
# here rather than escaping at 65 render sites is deliberate: the boundary is
# one place, and an author hears about the name while writing it rather than
# during a deploy. `column_definition` already makes the same trade for a
# default it cannot spell.
#
# `'`, a space, mixed case and non-ASCII are all rendered correctly and are
# NOT refused. A check that rejected anything unusual would be worse than the
# defect, because it would reject working schemas.
UNRENDERABLE_IN_A_NAME = {
    '"': "the appliers do not escape a double quote inside a quoted name",
    ";": "the SQLite and Turso appliers split rendered SQL on a semicolon",
}


def _unrenderable(kind: str, owner: str, name: str) -> list[str]:
    """Every character in `name` this package cannot write into SQL."""
    return [
        f"{kind} '{name}' in '{owner}' cannot be migrated: it contains "
        f"'{character}', and {reason}. Rename it."
        for character, reason in UNRENDERABLE_IN_A_NAME.items()
        if character in name
    ]


def _reported(holds: bool, message: str) -> list[str]:
    """The message when the condition holds, and nothing when it does not.

    A validator's whole job is collecting problems, so every finding here is
    one message and one condition. Written as a filter, the condition and the
    message it produces sit on the same expression and the list is built
    rather than appended to.
    """
    return [message for held in [holds] if held]


def validate_schema(schema: Schema) -> ValidationResult:
    """
    Validate a schema for correctness.

    Checks:
    - All foreign key references point to existing tables/columns
    - All index columns exist in the table
    - Type names are recognized
    - No obvious circular dependencies

    Args:
        schema: Schema to validate

    Returns:
        A ValidationResult, whose two lists are reached by name
        - warnings: Non-fatal issues (unknown types, etc.)
        - errors: Fatal issues (invalid references, etc.)
    """
    checked = [_validate_table(table_name, table, schema) for table_name, table in schema.items()]

    return {
        "warnings": [message for result in checked for message in result["warnings"]]
        + _check_circular_dependencies(schema),
        "errors": [message for result in checked for message in result["errors"]],
    }


def _the_empty_table(
    table_name: str, _table: Table, _columns: dict[str, Column], _schema: Schema
) -> ValidationResult:
    """A TABLE WITH NO COLUMNS IS NOT AN EMPTY TABLE, IT IS A BROKEN ONE.

    `table.get("columns", {})` accepted the absence and the schema validated
    clean, then the differ emitted `create_table` and the renderer produced
    `CREATE TABLE "empty" (\\n    \\n)`, which SQLite answers with
    `near ")": syntax error`. Measured 2026-08-19.

    This is the second of the three fallbacks three BDD schema helpers were
    deleted for on 2026-08-16. The first, a column with no type becoming TEXT,
    was removed from the TOML loader and from this module the same day.

    A TOML section whose `[users.columns]` header is misspelt, or columns
    nested one level wrong, produces a table the author never described, and
    nothing objects until the engine names a parenthesis.

    Nothing else about the table is checked, because every other check reads
    the columns. An index or a primary key naming a column that cannot exist
    would report the same absence a second and third time.
    """
    return {
        "warnings": [],
        "errors": _unrenderable("Table", table_name, table_name)
        + [
            f"Table '{table_name}': no columns. A table needs at least "
            f'one, and this renders as `CREATE TABLE "{table_name}" ()`, '
            f"which no engine accepts."
        ],
    }


def _the_populated_table(
    table_name: str, table: Table, columns: dict[str, Column], schema: Schema
) -> ValidationResult:
    """Every finding about one table that declares at least one column."""
    inspected = [
        (col_name, _validate_column(table_name, col_name, col_def, schema))
        for col_name, col_def in columns.items()
    ]

    return {
        "warnings": [message for _, result in inspected for message in result["warnings"]],
        "errors": _unrenderable("Table", table_name, table_name)
        + [
            message
            for col_name, result in inspected
            for message in _unrenderable("Column", table_name, col_name) + result["errors"]
        ]
        + [
            message
            for idx_name, idx_def in table.get("indexes", {}).items()
            for message in _validate_index(table_name, idx_name, idx_def, columns)
        ]
        + [
            f"Table '{table_name}': Primary key column '{pk_col}' does not exist"
            for pk_col in table.get("primary_key", [])
            if pk_col not in columns
        ],
    }


_WHEN_THE_TABLE_HAS_COLUMNS: dict[
    bool, Callable[[str, Table, dict[str, Column], Schema], ValidationResult]
] = {
    True: _the_populated_table,
    False: _the_empty_table,
}


def _validate_table(table_name: str, table: Table, schema: Schema) -> ValidationResult:
    """Validate one table: its name, its columns, its indexes and its key."""
    columns = table.get("columns", {})
    return _WHEN_THE_TABLE_HAS_COLUMNS[bool(columns)](table_name, table, columns, schema)


def _the_typeless_column(
    table_name: str, col_name: str, col_def: Column, _schema: Schema
) -> ValidationResult:
    """A COLUMN WITH NO TYPE IS AN ERROR, AND THIS USED TO SUPPLY ONE.

    It read `col_def.get("type", "text")`, so the validator answered a
    typeless column with the same guess every renderer makes and reported
    nothing. Measured 2026-08-19: `column_definition("c", {}, dialect)`
    returns TEXT on all three engines and the reconstruction renderer does
    too, so a schema built through the API produced a TEXT column everywhere
    and validated clean.

    TEXT is the worst possible guess because it accepts every value. The
    column takes integers, timestamps and JSON without complaint and stores
    each as a string, and the first sign is a comparison or a sort behaving
    oddly in production.

    The renderers keep their defaults. Three of them each guarding themselves
    is the same question answered in three places; this is the boundary, and
    every path that writes DDL asks it.

    Nothing else about the column is checked, because every other check reads
    the type or reports on top of it.
    """
    return {
        "warnings": [],
        "errors": [
            f"Table '{table_name}', column '{col_name}': no type. It carries "
            f"{sorted(col_def) or 'nothing'}, and a column without a type is "
            f"rendered as TEXT by every engine, which accepts any value."
        ],
    }


def _the_typed_column(
    table_name: str, col_name: str, col_def: Column, schema: Schema
) -> ValidationResult:
    """Every finding about one column that declares a type."""
    col_type = col_def["type"]

    # Normalize type for checking (remove size specifiers)
    base_type = col_type.lower().split("(")[0].strip()

    # AN UNKNOWN TYPE IS A WARNING, AND THE OLD TEXT READ AS REASSURANCE.
    # It said "may be custom type or enum" and stopped there. It is still a
    # warning rather than an error, because a custom PostgreSQL type is
    # legitimate and this function is not told which engine it is validating
    # for. What it did not say is what an unrecognised name costs on the
    # other two.
    #
    # SQLite assigns column affinity by matching substrings of the declared
    # name, and a name it recognises nothing in gets NUMERIC. Measured
    # 2026-08-19 with `text` mistyped as `txet`:
    #
    #     CREATE TABLE t (good TEXT, typo TXET)
    #     INSERT INTO t VALUES ('007', '007')
    #     good  '007' as text
    #     typo  7     as integer
    #
    # The string written was '007' and the value stored is the number 7. The
    # leading zeros are gone and the sort order changes with them.
    unknown_type = _reported(
        base_type not in KNOWN_TYPES and not base_type.endswith("[]"),
        f"Table '{table_name}', column '{col_name}': unknown type "
        f"'{col_type}'. It may be a custom type or an enum, and if it is "
        f"a typo SQLite and Turso will give the column NUMERIC affinity "
        f"and store '007' as the number 7.",
    )

    # A PRIMARY KEY that was DECLARED nullable is a contradiction the author
    # wrote down, and no engine can honour it. An omitted `nullable` is not:
    # declaring the key has already said NOT NULL. This used to read
    # `col_def.get("nullable", True)` and so warned about the schema this
    # package ships, telling the author to repeat themselves.
    nullable_key = _reported(
        bool(col_def.get("primary_key")) and col_def.get("nullable") is True,
        f"Table '{table_name}', column '{col_name}': Primary key column should be NOT NULL",
    )

    # Only a NEW column can fail against rows that already exist.
    new_not_null = _reported(
        bool(col_def.get("is_new")) and not is_nullable(col_def) and "default" not in col_def,
        f"Table '{table_name}', column '{col_name}': "
        f"New NOT NULL column without default may fail if table has data",
    )

    # THE FOUR ACTIONS THE TYPE DECLARES, AND NOTHING ELSE WAS CHECKED.
    # `Column.on_delete` is Literal["cascade", "set null", "restrict",
    # "no action"], and `validate_schema` accepted `set_null`, `CASCADE` and
    # `nonsense` alike, with no error and no warning.
    #
    # An unrecognised value does not fail. The renderer writes it out after
    # `.upper().replace("_", " ")`, so `set_null` reaches the engine as the
    # correct SET NULL and the table is created properly. Introspection reads
    # the engine's SET NULL back and normalises it to `set null`, which does
    # not equal the declared `set_null`, so the differ sees a change:
    #
    #     second deploy  drop_foreign_key pid, add_foreign_key pid
    #
    # On SQLite and Turso both of those go through table reconstruction, so
    # every deploy rebuilds the whole table, copies every row, and reports
    # success. Measured 2026-08-19. It never converges and never complains.
    #
    # The vocabulary is read from the type rather than listed, because the
    # whole failure is a declaration and a check disagreeing about what the
    # values are.
    actions = [
        f"Table '{table_name}', column '{col_name}': {action_key} "
        f"{col_def[action_key]!r} is not one of {sorted(allowed)}. An "
        f"unrecognised action still writes correct SQL and then never "
        f"matches what is read back, so the table is rebuilt on every "
        f"deploy."
        for action_key in ("on_delete", "on_update")
        if action_key in col_def
        for allowed in [Column.__annotations__[action_key].__args__]
        if col_def[action_key] not in allowed
    ]

    references = [
        message
        for reference in [col_def[key] for key in ("references",) if key in col_def]
        for message in [_validate_reference(table_name, col_name, reference, schema)]
        if message
    ]

    return {
        "warnings": unknown_type + nullable_key + new_not_null,
        "errors": actions + references,
    }


_WHEN_THE_COLUMN_HAS_A_TYPE: dict[bool, Callable[[str, str, Column, Schema], ValidationResult]] = {
    True: _the_typed_column,
    False: _the_typeless_column,
}


def _validate_column(
    table_name: str,
    col_name: str,
    col_def: Column,
    schema: Schema,
) -> ValidationResult:
    """Validate a single column definition."""
    return _WHEN_THE_COLUMN_HAS_A_TYPE[bool(col_def.get("type"))](
        table_name, col_name, col_def, schema
    )


def _validate_reference(
    table_name: str,
    col_name: str,
    reference: str,
    schema: Schema,
) -> str | None:
    """The first thing wrong with a foreign key reference, or None.

    ONE PROBLEM, NOT ALL OF THEM. A reference with no dot has no table name
    to look up, so reporting the missing table beside the bad format would
    name a table the author never wrote.
    """
    where = f"Table '{table_name}', column '{col_name}': "
    ref_table, dot, ref_col = reference.partition(".")

    named = [schema[name] for name in [ref_table] if dot and name in schema]
    ref_columns = [name for table in named for name in table.get("columns", {})]

    problems = (
        _reported(
            not dot,
            f"{where}Invalid reference format '{reference}' (expected 'table.column')",
        )
        + _reported(
            bool(dot) and ref_table not in schema,
            f"{where}References non-existent table '{ref_table}'",
        )
        + _reported(
            bool(named) and ref_col not in ref_columns,
            f"{where}References non-existent column '{ref_table}.{ref_col}'",
        )
    )
    return [*problems, None][0]


def _validate_index(
    table_name: str,
    idx_name: str,
    idx_def: Index,
    columns: dict[str, Column],
) -> list[str]:
    """Validate an index definition.

    An index with no columns reports once. The second list is empty in that
    case anyway, because there is nothing to walk.
    """
    idx_columns = idx_def.get("columns", [])

    return _reported(
        not idx_columns,
        f"Table '{table_name}', index '{idx_name}': Index must have at least one column",
    ) + [
        f"Table '{table_name}', index '{idx_name}': Column '{col}' does not exist"
        for col in idx_columns
        if col not in columns
    ]


def _foreign_key_graph(schema: Schema) -> dict[str, set[str]]:
    """Which tables each table's foreign keys point at.

    A SELF-REFERENCE IS NOT AN EDGE: a tree whose `parent_id` points at its
    own table is ordinary, and counting it would report a cycle in a schema
    that has none.

    A reference to a table the schema does not declare is left out too. It
    has no outgoing edges of its own, so it can never close a loop, and
    `validate_schema` already reports it as an error by name.
    """
    return {
        table_name: {
            ref
            for col_def in table.get("columns", {}).values()
            if "references" in col_def
            for ref in [col_def["references"].split(".")[0]]
            if ref != table_name and ref in schema
        }
        for table_name, table in schema.items()
    }


# One walk of the graph carries the cycle it has found, which is empty until
# it finds one, and the set of tables it has already walked.
_Walked = tuple[list[str], frozenset[str]]


def _the_closed_loop(
    node: str, path: tuple[str, ...], walked: frozenset[str], _deps: dict[str, set[str]]
) -> _Walked:
    """This table is already on the path, so the walk has come back around."""
    return [*path[path.index(node) :], node], walked


def _the_walked_table(
    _node: str,
    _path: tuple[str, ...],
    walked: frozenset[str],
    _deps: dict[str, set[str]],
) -> _Walked:
    """This table was walked already and held no cycle, so it holds none now."""
    return [], walked


def _the_new_table(
    node: str, path: tuple[str, ...], walked: frozenset[str], deps: dict[str, set[str]]
) -> _Walked:
    """Walk what this table points at, keeping the first cycle any of them meets.

    Sorted, so a schema with two cycles names the same one on every run. The
    old code took the dependencies in set order, which changes with the hash
    seed.
    """
    return reduce(
        partial(_the_next_dependency, path=(*path, node), deps=deps),
        sorted(deps[node]),
        ([], walked | {node}),
    )


# WHERE A TABLE STANDS IN THE WALK, AS A TABLE RATHER THAN THREE BRANCHES.
# Being on the current path is what closes a loop and is asked first, so it
# answers the same way whether or not the table has been walked before.
_WHERE_THE_TABLE_STANDS: dict[
    tuple[bool, bool],
    Callable[[str, tuple[str, ...], frozenset[str], dict[str, set[str]]], _Walked],
] = {
    (True, True): _the_closed_loop,
    (True, False): _the_closed_loop,
    (False, True): _the_walked_table,
    (False, False): _the_new_table,
}


def _walk(
    node: str, path: tuple[str, ...], walked: frozenset[str], deps: dict[str, set[str]]
) -> _Walked:
    """One step of the depth-first walk."""
    return _WHERE_THE_TABLE_STANDS[(node in path, node in walked)](node, path, walked, deps)


def _the_cycle_in_hand(
    found: _Walked, _dep: str, _path: tuple[str, ...], _deps: dict[str, set[str]]
) -> _Walked:
    """A cycle is already found, so the remaining dependencies are not walked."""
    return found


def _the_unwalked_dependency(
    found: _Walked, dep: str, path: tuple[str, ...], deps: dict[str, set[str]]
) -> _Walked:
    """Nothing found yet, so this dependency is walked from here."""
    return _walk(dep, path, found[1], deps)


_WHEN_A_CYCLE_IS_FOUND: dict[
    bool,
    Callable[[_Walked, str, tuple[str, ...], dict[str, set[str]]], _Walked],
] = {
    True: _the_cycle_in_hand,
    False: _the_unwalked_dependency,
}


def _the_next_dependency(
    found: _Walked, dep: str, *, path: tuple[str, ...], deps: dict[str, set[str]]
) -> _Walked:
    """Walk one more dependency, unless a cycle is already in hand."""
    return _WHEN_A_CYCLE_IS_FOUND[bool(found[0])](found, dep, path, deps)


def _cycle_from(start: str, deps: dict[str, set[str]]) -> list[str]:
    """The first cycle reachable from `start`, as the path around it."""
    return _walk(start, (), frozenset(), deps)[0]


# What the scan of the schema carries: the tables already named in a cycle,
# and the warnings written so far.
_Scanned = tuple[frozenset[str], tuple[str, ...]]


def _the_table_in_a_cycle(
    found: _Scanned, _table_name: str, _deps: dict[str, set[str]]
) -> _Scanned:
    """This table was already named in a cycle, so it is not walked again."""
    return found


def _no_cycle_here(found: _Scanned, _cycle: list[str]) -> _Scanned:
    """The walk met no cycle, so nothing is recorded."""
    return found


def _the_recorded_cycle(found: _Scanned, cycle: list[str]) -> _Scanned:
    """Report the cycle once, and do not walk its members again."""
    named, warnings = found
    return named | set(cycle[:-1]), (
        *warnings,
        f"Circular foreign key dependency detected: {' -> '.join(cycle)}. "
        f"This may require deferrable constraints.",
    )


_WHEN_THE_WALK_MET_A_CYCLE: dict[bool, Callable[[_Scanned, list[str]], _Scanned]] = {
    True: _the_recorded_cycle,
    False: _no_cycle_here,
}


def _the_unchecked_table(found: _Scanned, table_name: str, deps: dict[str, set[str]]) -> _Scanned:
    """Walk from this table and record whatever cycle it meets."""
    cycle = _cycle_from(table_name, deps)
    return _WHEN_THE_WALK_MET_A_CYCLE[bool(cycle)](found, cycle)


_WHEN_THE_TABLE_IS_NAMED: dict[bool, Callable[[_Scanned, str, dict[str, set[str]]], _Scanned]] = {
    True: _the_table_in_a_cycle,
    False: _the_unchecked_table,
}


def _the_next_table(found: _Scanned, table_name: str, *, deps: dict[str, set[str]]) -> _Scanned:
    """One table of the schema, skipped when a cycle has already named it."""
    return _WHEN_THE_TABLE_IS_NAMED[table_name in found[0]](found, table_name, deps)


def _check_circular_dependencies(schema: Schema) -> list[str]:
    """
    Check for circular foreign key dependencies.

    Returns warnings (not errors) because circular dependencies
    can be valid with deferrable constraints.
    """
    deps = _foreign_key_graph(schema)
    _named, warnings = reduce(partial(_the_next_table, deps=deps), schema, (frozenset(), ()))
    return list(warnings)


def _refuse_the_invalid_schema(errors: list[str]) -> list[str]:
    """Errors were found, so the schema is refused and they are all named."""
    error_list = "\n  - ".join(errors)
    raise ValidationError(
        f"Schema validation failed with {len(errors)} error(s):\n  - {error_list}",
        table=NOTHING,
        column=NOTHING,
        reference=NOTHING,
    )


def _the_valid_schema(errors: list[str]) -> list[str]:
    """No errors, so the schema stands and the empty list comes back."""
    return errors


_WHEN_THE_SCHEMA_IS_INVALID: dict[bool, Callable[[list[str]], list[str]]] = {
    True: _refuse_the_invalid_schema,
    False: _the_valid_schema,
}


def validate_schema_strict(schema: Schema) -> None:
    """
    Validate schema and raise on any error.

    Args:
        schema: Schema to validate

    Raises:
        ValidationError: If any validation errors found
    """
    errors = validate_schema(schema)["errors"]
    _WHEN_THE_SCHEMA_IS_INVALID[bool(errors)](errors)
