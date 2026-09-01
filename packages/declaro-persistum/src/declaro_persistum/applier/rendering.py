"""The supervisor: one operation in, the statements that run it out.

A MIGRATION IS A COMPOSITION, and this is where the composing happens. Most
operations are one row of `templates.TEMPLATES` filled from the operation and
rendered. A few are several rows rendered in turn, with the supervisor working
out which ones and passing the values between them. Nothing else in the package
decides what an operation means.

WHAT THE SUPERVISORS BELOW ACTUALLY DO. Each takes an operation and returns
statements, and each reaches `render` the same way any other caller does, with
named values checked against the row's own list.

  alter_column      PostgreSQL: one statement per property that changed
  add_constraint    PostgreSQL: the kind of constraint picks the row
  drop_foreign_key  PostgreSQL: two names, because a key made inline by CREATE
                    TABLE carries the engine's name and one made by us carries
                    ours, and only one of them exists
  create_view       every engine: a materialized view on SQLite is emulated
  drop_view         every engine: the same, in reverse
  add_constraint    SQLite and Turso: refused, because the engine has no such
  drop_constraint   statement and this package does not yet rebuild a table
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from declaro_persistum.applier.statements import Rendering, render, rendered
from declaro_persistum.applier.templates import TEMPLATES
from declaro_persistum.boundary import helper
from declaro_persistum.types import Engine, Operation, written_as

Composer = Callable[[Operation, Engine], list[str]]


# =============================================================================
# PostgreSQL alters a column where it stands
# =============================================================================
#
# Which statements run depends on what changed, so the supervisor decides and
# hands the values to `render`. The rows themselves declare names with no
# filler, because there is nowhere in the operation to read them from.


@helper
def _the_new_half(change: dict[str, Any]) -> Any:
    """The differ wrote a pair, so the new value is the `to` half."""
    return change.get("to", change)


@helper
def _the_value_itself(change: Any) -> Any:
    """The differ wrote the new value bare, so it is the whole change."""
    return change


_CHANGE_SHAPE: dict[bool, Callable[[Any], Any]] = {
    True: _the_new_half,
    False: _the_value_itself,
}


@helper
def _changed_to(change: Any) -> Any:
    """The new value of a change the differ wrote as `{"from": x, "to": y}`.

    A change is sometimes the bare new value and sometimes that pair. Reading
    `.lower()` off the pair is what crashed `_map_type` on a `Literal` column.
    """
    return _CHANGE_SHAPE[written_as(dict, change)](change)


@helper
def _alter_type(table: str, column: str, change: Any, engine: Engine) -> list[str]:
    """The column's new type, with a cast so the rows already there convert."""
    return render(
        "alter_column:type",
        TEMPLATES[engine]["alter_column:type"],
        table=table,
        column=column,
        type=_changed_to(change),
    )


@helper
def _alter_nullable(table: str, column: str, change: Any, engine: Engine) -> list[str]:
    """Whether the column accepts NULL, which is two different statements."""
    row = _NULLABILITY[bool(_changed_to(change))]
    return render(row, TEMPLATES[engine][row], table=table, column=column)


_NULLABILITY: dict[bool, str] = {
    True: "alter_column:null",
    False: "alter_column:not_null",
}


@helper
def _alter_default(table: str, column: str, change: Any, engine: Engine) -> list[str]:
    """A default set, or a default removed."""
    value = _changed_to(change)
    return _DEFAULT_CHANGE[value is None](table, column, value, engine)


@helper
def _drop_the_default(table: str, column: str, _value: Any, engine: Engine) -> list[str]:
    """The column has no default any more."""
    return render(
        "alter_column:no_default",
        TEMPLATES[engine]["alter_column:no_default"],
        table=table,
        column=column,
    )


@helper
def _set_the_default(table: str, column: str, value: Any, engine: Engine) -> list[str]:
    """The column's new default, written as the declaration spells it."""
    return render(
        "alter_column:default",
        TEMPLATES[engine]["alter_column:default"],
        table=table,
        column=column,
        default=value,
    )


_DEFAULT_CHANGE: dict[bool, Callable[[str, str, Any, Engine], list[str]]] = {
    True: _drop_the_default,
    False: _set_the_default,
}


@helper
def _alter_unique(table: str, column: str, change: Any, engine: Engine) -> list[str]:
    """UNIQUE is a table constraint on this engine, not a column property.

    The constraint is named for the table and the column so that dropping it
    later can find it. Before this was handled at all, making a column unique
    produced an alter_column nothing recognised, which rendered an empty
    string, which asyncpg failed on deep inside its own protocol.
    """
    row = _UNIQUE_CHANGE[bool(_changed_to(change))]
    return render(
        row,
        TEMPLATES[engine][row],
        table=table,
        column=column,
        constraint=f"{table}_{column}_key",
    )


_UNIQUE_CHANGE: dict[bool, str] = {
    True: "alter_column:unique",
    False: "alter_column:not_unique",
}


# The order is the order the statements run in, and it matters: a type change
# has to land before a default that is spelled in the new type.
_ALTER_COLUMN: dict[str, Callable[[str, str, Any, Engine], list[str]]] = {
    "type": _alter_type,
    "nullable": _alter_nullable,
    "default": _alter_default,
    "unique": _alter_unique,
}


@helper
def _the_alter_statements(
    _table: str, _column: str, _changes: dict[str, Any], statements: list[str]
) -> list[str]:
    """At least one property this engine can alter was named."""
    return statements


@helper
def _refuse_empty_alter_column(
    table: str, column: str, changes: dict[str, Any], _statements: list[str]
) -> list[str]:
    """AN OPERATION THAT RENDERS NOTHING IS A BUG, NOT A NO-OP.

    Returning an empty string sent it to `connection.execute`, and asyncpg
    does not refuse it: it fails inside the protocol with an AttributeError
    about `decode`, naming nothing an operator could act on.
    """
    raise ValueError(
        f"No PostgreSQL statement for an alter_column on {table!r}.{column!r}: "
        f"this renderer handles {list(_ALTER_COLUMN)} and the operation asked "
        f"for {sorted(changes)}."
    )


_WHEN_ALTERED: dict[bool, Callable[[str, str, dict[str, Any], list[str]], list[str]]] = {
    True: _the_alter_statements,
    False: _refuse_empty_alter_column,
}


@helper
def _postgresql_alter_column(operation: Operation, engine: Engine) -> list[str]:
    """Every property that changed, each as its own statement."""
    table = operation["table"]
    column = operation["details"]["column"]
    changes = operation["details"]["changes"]

    statements = [
        statement
        for prop, write in _ALTER_COLUMN.items()
        if prop in changes
        for statement in write(table, column, changes[prop], engine)
    ]

    return _WHEN_ALTERED[bool(statements)](table, column, changes, statements)


# =============================================================================
# PostgreSQL adds a constraint to a table that already exists
# =============================================================================


@helper
def _added_check(table: str, name: str, definition: dict[str, Any], engine: Engine) -> list[str]:
    """A CHECK, added after the fact."""
    return render(
        "add_constraint:check",
        TEMPLATES[engine]["add_constraint:check"],
        table=table,
        constraint=name,
        expression=definition.get("expression", ""),
    )


@helper
def _added_unique(table: str, name: str, definition: dict[str, Any], engine: Engine) -> list[str]:
    """A UNIQUE over the columns the declaration names."""
    return render(
        "add_constraint:unique",
        TEMPLATES[engine]["add_constraint:unique"],
        table=table,
        constraint=name,
        columns=", ".join(f'"{c}"' for c in definition.get("columns", [])),
    )


_ADDED_CONSTRAINT: dict[str, Callable[[str, str, dict[str, Any], Engine], list[str]]] = {
    "check": _added_check,
    "unique": _added_unique,
}


@helper
def _the_added_constraint(
    table: str, name: str, kind: Any, definition: dict[str, Any], engine: Engine
) -> list[str]:
    """A kind this engine writes, so the table names the writer."""
    return _ADDED_CONSTRAINT[kind](table, name, definition, engine)


@helper
def _refuse_added_constraint(
    _table: str, _name: str, kind: Any, _definition: dict[str, Any], _engine: Engine
) -> list[str]:
    """A kind this engine does not write, named so the author finds it."""
    raise ValueError(f"Unknown constraint type: {kind}. Written here: {sorted(_ADDED_CONSTRAINT)}.")


_KNOWN_ADDED_CONSTRAINT: dict[
    bool, Callable[[str, str, Any, dict[str, Any], Engine], list[str]]
] = {
    True: _the_added_constraint,
    False: _refuse_added_constraint,
}


@helper
def _postgresql_add_constraint(operation: Operation, engine: Engine) -> list[str]:
    """The kind of constraint picks the row that writes it."""
    definition = operation["details"]["definition"]
    kind = definition.get("type")
    return _KNOWN_ADDED_CONSTRAINT[kind in _ADDED_CONSTRAINT](
        operation["table"],
        operation["details"]["constraint"],
        kind,
        definition,
        engine,
    )


# =============================================================================
# PostgreSQL drops a foreign key by both names it can have
# =============================================================================


@helper
def _postgresql_drop_foreign_key(operation: Operation, engine: Engine) -> list[str]:
    """THE CONVENTION NAME IS ONLY HALF OF WHAT EXISTS.

    A key we created carries `fk_{table}_{column}_{ref_table}`. A key created
    inline by CREATE TABLE carries PostgreSQL's own `{table}_{column}_fkey`.
    Dropping only the first failed on every table made before we started
    naming them, with `constraint ... does not exist`. Both names are
    deterministic, so both are dropped rather than one being guessed at, and
    IF EXISTS is safe precisely because two are tried.
    """
    table = operation["table"]
    column = operation["details"]["column"]
    reference = operation["details"]["references"]
    referenced = reference.split(".")[0]

    return [
        statement
        for name in (
            f"fk_{table}_{column}_{referenced}",
            f"{table}_{column}_fkey",
        )
        for statement in render(
            "drop_foreign_key:one",
            TEMPLATES[engine]["drop_foreign_key:one"],
            table=table,
            constraint=name,
        )
    ]


# =============================================================================
# Views, on every engine
# =============================================================================


@helper
def _plain_view(operation: Operation, engine: Engine, verb: str) -> list[str]:
    """A view that is only a query, which every engine writes in one
    statement."""
    row = f"{verb}:plain"
    return rendered(row, TEMPLATES[engine][row], operation)


@helper
def _create_plain_view(operation: Operation, engine: Engine) -> list[str]:
    """One statement, spelled as this engine spells a replaceable view."""
    return _plain_view(operation, engine, "create_view")


@helper
def _drop_plain_view(operation: Operation, engine: Engine) -> list[str]:
    """One statement, and IF EXISTS so a repeat is not an error."""
    return _plain_view(operation, engine, "drop_view")


@helper
def _create_materialized_view(operation: Operation, engine: Engine) -> list[str]:
    """PostgreSQL has the statement; SQLite and Turso build a table.

    A MATERIALIZED VIEW ON SQLITE IS NOT A VIEW. The emulation creates a table,
    the triggers that keep it current, and a registry row that records what it
    stands for. `DROP VIEW` could not remove it, which left the object behind
    and the schema reporting a view the database no longer had.
    """
    return _MATERIALIZED_CREATE[engine](operation, engine)


@helper
def _drop_materialized_view(operation: Operation, engine: Engine) -> list[str]:
    """The same, in reverse."""
    return _MATERIALIZED_DROP[engine](operation, engine)


@helper
def _emulated_materialized_view(operation: Operation, _engine: Engine) -> list[str]:
    """The table, its triggers, and its registry row."""
    from declaro_persistum.applier.sqlite import generate_create_view

    return generate_create_view(operation["details"])  # type: ignore[arg-type]


@helper
def _drop_emulated_materialized_view(operation: Operation, _engine: Engine) -> list[str]:
    """The table standing in for the view, and its registry row."""
    from declaro_persistum.applier.sqlite import generate_drop_view

    return generate_drop_view(operation["details"]["name"], materialized=True)


@helper
def _native_materialized_view(operation: Operation, engine: Engine) -> list[str]:
    """PostgreSQL holds the rows itself, so it is one statement."""
    row = "create_view:materialized"
    return rendered(row, TEMPLATES[engine][row], operation)


@helper
def _drop_native_materialized_view(operation: Operation, engine: Engine) -> list[str]:
    """DROP MATERIALIZED VIEW, which is a different statement from DROP VIEW.

    Emitting DROP VIEW against a materialized one is refused with
    WrongObjectTypeError rather than ignored.
    """
    row = "drop_view:materialized"
    return rendered(row, TEMPLATES[engine][row], operation)


_MATERIALIZED_CREATE: dict[str, Composer] = {
    "sqlite": _emulated_materialized_view,
    "turso": _emulated_materialized_view,
    "postgresql": _native_materialized_view,
}

_MATERIALIZED_DROP: dict[str, Composer] = {
    "sqlite": _drop_emulated_materialized_view,
    "turso": _drop_emulated_materialized_view,
    "postgresql": _drop_native_materialized_view,
}

_CREATE_VIEW: dict[bool, Composer] = {
    False: _create_plain_view,
    True: _create_materialized_view,
}

_DROP_VIEW: dict[bool, Composer] = {
    False: _drop_plain_view,
    True: _drop_materialized_view,
}


@helper
def _create_view(operation: Operation, engine: Engine) -> list[str]:
    """Whether the view holds its rows decides everything about it."""
    return _CREATE_VIEW[bool(operation["details"].get("materialized"))](operation, engine)


@helper
def _drop_view(operation: Operation, engine: Engine) -> list[str]:
    """The same question, asked again on the way out."""
    return _DROP_VIEW[bool(operation["details"].get("materialized"))](operation, engine)


# =============================================================================
# What SQLite and Turso refuse
# =============================================================================


@helper
def _refuse_add_constraint(operation: Operation, _engine: Engine) -> list[str]:
    """No ADD CONSTRAINT on this engine, and no table rebuild for one yet."""
    raise NotImplementedError(
        f"Cannot add constraint {operation['details']['constraint']!r} to "
        f"{operation['table']!r}: SQLite and Turso have no ADD CONSTRAINT, and "
        f"this package does not yet rebuild a table to add one. Declare the "
        f"constraint in the model before the table is first created, or add it "
        f"by hand."
    )


@helper
def _refuse_drop_constraint(operation: Operation, _engine: Engine) -> list[str]:
    """No DROP CONSTRAINT on this engine either."""
    raise NotImplementedError(
        f"Cannot drop constraint {operation['details']['constraint']!r} from "
        f"{operation['table']!r}: SQLite and Turso have no DROP CONSTRAINT, and "
        f"this package does not yet rebuild a table to remove one. Drop it by "
        f"hand, or recreate the table."
    )


# =============================================================================
# The supervisors, by engine
# =============================================================================

_SQLITE_COMPOSERS: dict[str, Composer] = {
    "create_view": _create_view,
    "drop_view": _drop_view,
    "add_constraint": _refuse_add_constraint,
    "drop_constraint": _refuse_drop_constraint,
}

COMPOSERS: dict[str, dict[str, Composer]] = {
    "sqlite": _SQLITE_COMPOSERS,
    "turso": _SQLITE_COMPOSERS,
    "postgresql": {
        "create_view": _create_view,
        "drop_view": _drop_view,
        "alter_column": _postgresql_alter_column,
        "add_constraint": _postgresql_add_constraint,
        "drop_foreign_key": _postgresql_drop_foreign_key,
    },
}


# =============================================================================
# One operation, rendered
# =============================================================================


@helper
def _composed(operation: Operation, engine: Engine) -> list[str]:
    """An operation whose statements depend on what it says."""
    return COMPOSERS[engine][operation["op"]](operation, engine)


@helper
def _from_template(operation: Operation, engine: Engine) -> list[str]:
    """An operation that is one row of the table, filled and rendered."""
    op_type = operation["op"]
    return rendered(op_type, TEMPLATES[engine][op_type], operation)


@helper
def _refuse_unknown_operation(operation: Operation, engine: Engine) -> list[str]:
    """An operation no engine row covers, named beside the ones that exist."""
    raise ValueError(
        f"No {engine} statement for operation {operation['op']!r}. "
        f"Handled: {sorted(set(TEMPLATES[engine]) | set(COMPOSERS[engine]))}."
    )


_HOW_TO_RENDER: dict[str, Callable[[Operation, Engine], list[str]]] = {
    "composed": _composed,
    "template": _from_template,
    "unknown": _refuse_unknown_operation,
}


@helper
def _which_way(operation: Operation, engine: Engine) -> str:
    """Composed, from a template, or neither."""
    op_type = operation["op"]
    return next(
        way
        for way, holds in (
            ("composed", op_type in COMPOSERS[engine]),
            ("template", op_type in TEMPLATES[engine]),
            ("unknown", True),
        )
        if holds
    )


@helper
def statements_for(operation: Operation, engine: Engine) -> list[str]:
    """The statements this operation runs on this engine, already separated.

    A LIST, NEVER A JOINED STRING. The appliers used to take a generated
    string and cut it with `str.split(";")`, which is wrong twice: a trigger
    body carries semicolons between BEGIN and END, and so does any string
    literal holding one. Both arrived in real declarations.
    """
    return _HOW_TO_RENDER[_which_way(operation, engine)](operation, engine)


@helper
def generate_operation_sql(operation: Operation, engine: Engine) -> str:
    """The same statements as one block, for a person to read.

    Joining is safe here and was not safe on the execution path, because
    nothing parses this back. It is what `--dry-run` prints.
    """
    return ";\n".join(statements_for(operation, engine))


__all__ = [
    "COMPOSERS",
    "Composer",
    "Rendering",
    "generate_operation_sql",
    "statements_for",
]
