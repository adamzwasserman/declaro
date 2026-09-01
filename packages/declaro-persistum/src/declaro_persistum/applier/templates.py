"""Every statement this package writes, as text, one row per operation.

READ THIS FILE TO SEE WHAT WE EMIT. That was not possible before: the SQL
lived inside thirty-two functions across two modules, so comparing what
PostgreSQL writes with what SQLite writes meant reading both and holding them
in your head. The two tables sit side by side here, and the difference between
the engines is now a difference you can see.

WHAT A ROW IS. The text, and the names the text fills from, on the same line.
The names are also the record of what the operation must carry, which nothing
recorded before: `Operation.details` says `dict[str, Any]`, one part of the
package fills those bags and three read them. A row's `fills` is that contract,
and `statements.render` checks it on every call.

THE ENGINES DISAGREE IN FOUR PLACES and every one of them is visible below:

  add_index          PostgreSQL has USING; SQLite and Turso do not
  alter_column       PostgreSQL alters in place; the others rebuild the table
  add_constraint     PostgreSQL has ADD CONSTRAINT; the others refuse
  drop_foreign_key   PostgreSQL drops two names, because a key made inline by
                     CREATE TABLE carries the engine's own name and one made
                     by us carries ours

A constraint dropped on the floor by one engine and honoured by two is what
this shape is meant to prevent. It had already happened: a table-level
constraint was added to the shared renderer and PostgreSQL, which keeps its
own copy, went on ignoring it.
"""

from __future__ import annotations

from typing import Any

from declaro_persistum.applier.spelling import (
    column_definition,
    constraint_clause,
)
from declaro_persistum.applier.statements import (
    Rendering,
    built_by,
    clause,
    detail,
    flag,
    the_table,
)
from declaro_persistum.boundary import helper
from declaro_persistum.types import NOTHING, Engine, Operation

# =============================================================================
# The two values that have to be assembled
# =============================================================================


@helper
def _table_body(operation: Operation, engine: Engine) -> str:
    """Everything between the parentheses of a CREATE TABLE.

    One clause per column, then the primary key if it spans more than one
    column, then each table-level constraint. The length depends on the input,
    which is why this is a function and the other fifteen names are not.

    THE FILTER SITS ABOVE THE JOIN. A comprehension evaluates its clauses in
    order and `primary_key` can be absent, so a filter written below the
    expression it guards lets `None` reach `join`. That mistake was made three
    times in the code this replaces.
    """
    details = operation["details"]
    columns = details["columns"]
    primary_key = details["primary_key"]

    column_defs = [
        column_definition(name, definition, engine) for name, definition in columns.items()
    ]

    composite_key = [
        f"PRIMARY KEY ({', '.join(chr(34) + column + chr(34) for column in key)})"
        for key in [primary_key]
        if key and len(key) > 1
    ]

    constraints = [
        constraint_clause(name, definition)
        for name, definition in (details.get("constraints") or {}).items()
    ]

    return ",\n    ".join([*column_defs, *composite_key, *constraints])


@helper
def _column_clause(operation: Operation, engine: Engine) -> str:
    """One column, spelled for this engine, as ADD COLUMN wants it."""
    return column_definition(
        operation["details"]["column"], operation["details"]["definition"], engine
    )


@helper
def _quoted_columns(operation: Operation, _engine: Engine) -> str:
    """The columns an index covers, quoted and comma-separated."""
    return ", ".join(f'"{column}"' for column in operation["details"]["definition"]["columns"])


@helper
def _escaped_values(operation: Operation, _engine: Engine) -> list[str]:
    """The declared values of an enum, with SQL's own escape applied.

    THE APOSTROPHE IS DOUBLED, which is how SQL escapes it. These come from a
    `Literal[...]` in a models file rather than from user input, but `it's
    live` is ordinary English and would end the string literal early.
    """
    return [value.replace("'", "''") for value in operation["details"]["values"]]


# =============================================================================
# SQLite and Turso
# =============================================================================
#
# One table for both, because they take the same statements. Turso aims at
# SQLite compatibility and has reached it for everything written here; where
# it has not, the difference is in how a statement is run and not in what the
# statement says, which is why it belongs in the applier and not in this file.


@helper
# honest-code-allow: L1.21.1 - _sqlite_rows and _postgresql_rows ARE the tables this clause asks for, and TEMPLATES is the table of them. PostgreSQL's is already built from SQLite's with the differing rows replaced, so collapsing them would mean a table whose one column is a whole table. what_an_applier_says_about_itself.feature holds the reuse instead
def _sqlite_rows(engine: Engine) -> dict[str, Rendering]:
    """The rows for an engine that cannot alter a column in place."""
    return {
        "create_table": {
            "template": 'CREATE TABLE "{table}" (\n    {body}\n)',
            "reads": {"table": the_table(), "body": built_by(_table_body)(engine)},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "drop_table": {
            "template": 'DROP TABLE "{table}"',
            "reads": {"table": the_table()},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "rename_table": {
            "template": 'ALTER TABLE "{table}" RENAME TO "{new_name}"',
            "reads": {"table": the_table(), "new_name": detail("new_name")},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "add_column": {
            "template": 'ALTER TABLE "{table}" ADD COLUMN {column}',
            "reads": {
                "table": the_table(),
                "column": built_by(_column_clause)(engine),
            },
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "drop_column": {
            "template": 'ALTER TABLE "{table}" DROP COLUMN "{column}"',
            "reads": {"table": the_table(), "column": detail("column")},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "rename_column": {
            "template": ('ALTER TABLE "{table}" RENAME COLUMN "{from_column}" TO "{to_column}"'),
            "reads": {
                "table": the_table(),
                "from_column": detail("from_column"),
                "to_column": detail("to_column"),
            },
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "alter_column": {
            "template": "-- Table reconstruction required for {table} (altering a column)",
            "reads": {"table": the_table()},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "add_index": {
            "template": ('CREATE {unique}INDEX "{index}" ON "{table}" ({columns}){where}'),
            "reads": {
                "index": detail("index"),
                "table": the_table(),
                "columns": built_by(_quoted_columns)(engine),
                "unique": flag("definition", "unique", "UNIQUE "),
                "where": clause("definition", "where", " WHERE "),
            },
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "drop_index": {
            "template": 'DROP INDEX "{index}"',
            "reads": {"index": detail("index")},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "add_foreign_key": {
            "template": "-- Table reconstruction required for {table} (adding a foreign key)",
            "reads": {"table": the_table()},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "drop_foreign_key": {
            "template": "-- Table reconstruction required for {table} (dropping a foreign key)",
            "reads": {"table": the_table()},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "insert_enum_values": {
            "template": "INSERT INTO \"{table}\" (value) VALUES ('{values}')",
            "reads": {
                "table": the_table(),
                "values": built_by(_escaped_values)(engine),
            },
            "given": frozenset(),
            "repeat": "values",
        },
    }


# =============================================================================
# PostgreSQL
# =============================================================================


@helper
def _postgresql_rows(engine: Engine) -> dict[str, Rendering]:
    """The rows for the engine that can alter a column where it stands."""
    return {
        # `alter_column` is absent on purpose. PostgreSQL alters a column
        # where it stands, which is one statement per property that changed,
        # so `rendering` supervises it out of the `alter_column:*` rows below.
        **{op: row for op, row in _sqlite_rows(engine).items() if op != "alter_column"},
        "add_index": {
            "template": ('CREATE {unique}INDEX "{index}" ON "{table}"{using} ({columns}){where}'),
            "reads": {
                "index": detail("index"),
                "table": the_table(),
                "columns": built_by(_quoted_columns)(engine),
                "unique": flag("definition", "unique", "UNIQUE "),
                "using": clause("definition", "using", " USING "),
                "where": clause("definition", "where", " WHERE "),
            },
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "drop_constraint": {
            "template": 'ALTER TABLE "{table}" DROP CONSTRAINT "{constraint}"',
            "reads": {"table": the_table(), "constraint": detail("constraint")},
            "given": frozenset(),
            "repeat": NOTHING,
        },
        "add_foreign_key": {
            "template": (
                'ALTER TABLE "{table}" ADD CONSTRAINT "{name}" '
                'FOREIGN KEY ("{column}") REFERENCES "{ref_table}"("{ref_column}")'
                "{on_delete}{on_update}"
            ),
            "reads": {
                "table": the_table(),
                "name": _foreign_key_name,
                "column": detail("column"),
                "ref_table": _referenced_table,
                "ref_column": _referenced_column,
                "on_delete": _cascade("on_delete", " ON DELETE "),
                "on_update": _cascade("on_update", " ON UPDATE "),
            },
            "given": frozenset(),
            "repeat": NOTHING,
        },
    }


# =============================================================================
# The pieces PostgreSQL's foreign keys need
# =============================================================================


@helper
def _foreign_key_name(operation: Operation) -> str:
    """The name we give a foreign key, so that dropping it can find it."""
    details = operation["details"]
    reference = details["references"]
    return f"fk_{operation['table']}_{details['column']}_{reference.split('.')[0]}"


@helper
def _referenced_table(operation: Operation) -> str:
    """The table half of a `table.column` reference."""
    reference: str = operation["details"]["references"]
    return reference.split(".")[0]


@helper
def _referenced_column(operation: Operation) -> str:
    """The column half of a `table.column` reference."""
    reference: str = operation["details"]["references"]
    return reference.split(".")[1]


@helper
def _cascade(key: str, keyword: str) -> Any:
    """ON DELETE or ON UPDATE, written the way SQL spells it, or nothing.

    The declaration says `set_null`; SQL says `SET NULL`.
    """
    return lambda operation: "".join(
        f"{keyword}{action.upper().replace('_', ' ')}"
        for action in [operation["details"].get(key)]
        if action
    )


# =============================================================================
# The rows a supervisor renders, rather than the operation itself
# =============================================================================
#
# A NAME WITH NO FILLER HAS NOWHERE TO BE READ FROM. Which of these runs
# depends on what an alter_column changed, so `rendering` decides and passes
# the values in. The names are still declared, and `render` still requires
# every one of them, which is the whole point of writing them down.

_SUPERVISED: dict[str, Rendering] = {
    "alter_column:type": {
        "template": (
            'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE {type} USING "{column}"::{type}'
        ),
        "reads": {},
        "given": frozenset({"table", "column", "type"}),
        "repeat": NOTHING,
    },
    "alter_column:not_null": {
        "template": 'ALTER TABLE "{table}" ALTER COLUMN "{column}" SET NOT NULL',
        "reads": {},
        "given": frozenset({"table", "column"}),
        "repeat": NOTHING,
    },
    "alter_column:null": {
        "template": 'ALTER TABLE "{table}" ALTER COLUMN "{column}" DROP NOT NULL',
        "reads": {},
        "given": frozenset({"table", "column"}),
        "repeat": NOTHING,
    },
    "alter_column:default": {
        "template": ('ALTER TABLE "{table}" ALTER COLUMN "{column}" SET DEFAULT {default}'),
        "reads": {},
        "given": frozenset({"table", "column", "default"}),
        "repeat": NOTHING,
    },
    "alter_column:no_default": {
        "template": 'ALTER TABLE "{table}" ALTER COLUMN "{column}" DROP DEFAULT',
        "reads": {},
        "given": frozenset({"table", "column"}),
        "repeat": NOTHING,
    },
    "alter_column:unique": {
        "template": ('ALTER TABLE "{table}" ADD CONSTRAINT "{constraint}" UNIQUE ("{column}")'),
        "reads": {},
        "given": frozenset({"table", "column", "constraint"}),
        "repeat": NOTHING,
    },
    "alter_column:not_unique": {
        "template": 'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint}"',
        "reads": {},
        "given": frozenset({"table", "column", "constraint"}),
        "repeat": NOTHING,
    },
    "add_constraint:check": {
        "template": ('ALTER TABLE "{table}" ADD CONSTRAINT "{constraint}" CHECK ({expression})'),
        "reads": {},
        "given": frozenset({"table", "constraint", "expression"}),
        "repeat": NOTHING,
    },
    "add_constraint:unique": {
        "template": ('ALTER TABLE "{table}" ADD CONSTRAINT "{constraint}" UNIQUE ({columns})'),
        "reads": {},
        "given": frozenset({"table", "constraint", "columns"}),
        "repeat": NOTHING,
    },
    "drop_foreign_key:one": {
        "template": 'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint}"',
        "reads": {},
        "given": frozenset({"table", "constraint"}),
        "repeat": NOTHING,
    },
}


# =============================================================================
# Views
# =============================================================================
#
# A PLAIN VIEW IS A TEMPLATE ON EVERY ENGINE. A materialized one is not: on
# SQLite and Turso it is a table with triggers and a registry row beside it,
# which is why `rendering` supervises the materialized half and renders these
# rows for the plain half.
#
# The engines spell the plain create differently and each spelling is
# deliberate. PostgreSQL replaces a view where it stands; SQLite has no
# REPLACE for a view and takes IF NOT EXISTS instead. Rendering both through
# one row would have quietly given PostgreSQL SQLite's statement, which is the
# mistake this table caught the first time it was run against the old code.

_SQLITE_VIEWS: dict[str, Rendering] = {
    "create_view:plain": {
        "template": 'CREATE VIEW IF NOT EXISTS "{name}" AS\n{query}',
        "reads": {"name": detail("name"), "query": detail("query")},
        "given": frozenset(),
        "repeat": NOTHING,
    },
    "drop_view:plain": {
        "template": 'DROP VIEW IF EXISTS "{name}"',
        "reads": {"name": detail("name")},
        "given": frozenset(),
        "repeat": NOTHING,
    },
}

_POSTGRESQL_VIEWS: dict[str, Rendering] = {
    "create_view:plain": {
        "template": 'CREATE OR REPLACE VIEW "{name}" AS\n{query}',
        "reads": {"name": detail("name"), "query": detail("query")},
        "given": frozenset(),
        "repeat": NOTHING,
    },
    "create_view:materialized": {
        "template": 'CREATE MATERIALIZED VIEW "{name}" AS\n{query}',
        "reads": {"name": detail("name"), "query": detail("query")},
        "given": frozenset(),
        "repeat": NOTHING,
    },
    "drop_view:plain": {
        "template": 'DROP VIEW IF EXISTS "{name}"',
        "reads": {"name": detail("name")},
        "given": frozenset(),
        "repeat": NOTHING,
    },
    "drop_view:materialized": {
        "template": 'DROP MATERIALIZED VIEW IF EXISTS "{name}"',
        "reads": {"name": detail("name")},
        "given": frozenset(),
        "repeat": NOTHING,
    },
}


# =============================================================================
# The tables, by engine
# =============================================================================

TEMPLATES: dict[str, dict[str, Rendering]] = {
    "sqlite": {**_sqlite_rows("sqlite"), **_SQLITE_VIEWS},
    "turso": {**_sqlite_rows("sqlite"), **_SQLITE_VIEWS},
    "postgresql": {
        **_postgresql_rows("postgresql"),
        **_SUPERVISED,
        **_POSTGRESQL_VIEWS,
    },
}
