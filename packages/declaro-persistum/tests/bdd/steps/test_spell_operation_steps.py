"""Bind `tests/bdd/features/schema/spell_operation.feature`.

WHAT THE FEATURE ASKS FOR. `spell_operation` is handed one schema change and
the name of an engine, and hands back the statements that carry the change out
on that engine. One change is often several statements, so the answer is a
list, in the order the statements have to run. An engine that cannot express
the change hands back a fault instead, and the fault comes back in the return
rather than being raised.

WHAT RUNS AGAINST A REAL DATABASE, AND WHY. A statement naming a column the
table has not got is still a plausible string, so every scenario here that says
anything about rows, keys or indexes opens an in-memory SQLite database, runs
the spelled statements on it, and reads the answer out of the database. Nothing
here opens a file, a server or a network connection, and the function under test
is handed neither.

THIS FILE PLAYS THE APPLIER, AND SAYS SO. `spell_operation` writes statements;
somebody else runs them, in a transaction, with foreign key enforcement off
while a table is being rebuilt and a `PRAGMA foreign_key_check` before the
commit. That protocol is the applier's, because `PRAGMA foreign_keys` is a
no-op inside a transaction and so can never be one of the statements that come
back. `_carried_out` below is that applier, written once, and every rebuild
scenario goes through it.

NOTHING HERE READS THE PACKAGE'S SOURCE. A claim about how a module is written
is a claim the gates over `src/` make. These steps call the function and read
what comes back.

WHAT THE WIDER TYPES LET FOUR SCENARIOS SAY, SINCE 2026-09-21. A `Constraint`
carries the kind of rule it is, so a key over two columns is written down as one
rule naming both rather than as a facet on each of them. A `Reference` carries
what a delete does to the child, so a scenario asking for ON DELETE CASCADE has
somewhere to put it. And `remove_enum_values` is a word, so taking a value out
of a lookup is an operation this file can ask for by name.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from typing import Any, cast

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.spell_operation import spell_operation
from declaro_persistum.types import (
    Column,
    Condition,
    Constraint,
    Engine,
    Index,
    OnDelete,
    Operation,
    Reference,
    Statement,
    Table,
    Value,
)
from tests.conftest import NARROWS_BY_NOTHING, a_column, a_constraint

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/schema/spell_operation.feature")

A_FAULT = {"kind", "what_went_wrong", "what_to_do_instead"}
THE_ENGINES = ("sqlite", "postgresql", "turso")

# The lookup table a declared `Literal` becomes, for the table and column the
# enum scenarios use.
THE_STATUSES = "_dp_enum_orders_status"


# --- the pieces a scenario hands over ---------------------------------------


def _column(name: str, declared_type: str) -> Column:
    """One declared column, with every field a `Column` carries."""
    return a_column(
        name=name,
        declared_type=declared_type,
        renamed_from=None,
        references=None,
    )


def _rule(kind: str, column: str, value: Value = None) -> Constraint:
    """One rule about one column, named after the kind and the column it is on."""
    return a_constraint(
        name=f"{kind}_{column}",
        kind=kind,
        columns=[column],
        rule=NARROWS_BY_NOTHING,
        value=value,
    )


def _without(constraints: list[Constraint], column: str, kind: str) -> list[Constraint]:
    """The rules, with this column's rule of that kind taken out."""
    return [
        constraint
        for constraint in constraints
        if not (constraint["kind"] == kind and constraint["columns"] == [column])
    ]


def _ruling(
    constraints: list[Constraint], column: str, kind: str, value: Value = None
) -> list[Constraint]:
    """The rules, with this column's rule of that kind replaced by this one."""
    return [*_without(constraints, column, kind), _rule(kind, column, value)]


def _pointing_at(column: Column, parent: str | None, on_delete: OnDelete) -> Column:
    """The same column, pointing at a parent key or at nothing.

    A scenario writes the parent key the way SQL does, `authors.id`, and a
    `Reference` holds the table and the column apart, so the splitting happens
    here, once, where the sentence is read.
    """
    table, _, name = (parent or "").rpartition(".")
    pointing: dict[bool, Callable[[], Reference | None]] = {
        True: lambda: None,
        False: lambda: Reference(table=table, column=name, on_delete=on_delete),
    }
    return {**column, "references": pointing[parent is None]()}


def _operation(
    kind: str,
    table: str,
    detail: dict[str, Value],
    columns: list[Column],
    indexes: list[Index],
    constraints: list[Any],
    copying: list[str],
    values: list[Value],
) -> Operation:
    """One schema change, as the differ would hand it over.

    EVERY FIELD IS WRITTEN OUT, including the empty ones. An argument with a
    default would let a scenario forget a table's columns and be answered with
    an empty list, which reads exactly like a table that has none.
    """
    return {
        "kind": kind,  # type: ignore[typeddict-item]
        "table": table,
        "detail": detail,
        "columns": columns,
        "indexes": indexes,
        "constraints": constraints,
        "copying": copying,
        "values": values,
        "destroys": False,
    }


def _values(statement: Statement) -> tuple[Value, ...]:
    """The values beside a statement, in the order their marks appear."""
    return tuple(statement["parameters"].values())


def _run(connection: sqlite3.Connection, statements: list[Statement]) -> None:
    """Run spelled statements on a real database, in the order they came back."""
    for statement in statements:
        connection.execute(statement["text"], _values(statement))


def _what_the_engine_said(connection: sqlite3.Connection, statements: list[Statement]) -> list[str]:
    """Run the statements and keep the engine's complaint when it refuses one."""
    refused: list[str] = []
    try:
        _run(connection, statements)
    except sqlite3.IntegrityError as complaint:
        refused.append(str(complaint))
    return refused


def _spell_and_run(
    connection: sqlite3.Connection, operation: Operation, engine: Engine
) -> list[Statement]:
    """Spell one change and carry it out, so a broken statement fails here."""
    spelled = spell_operation(operation, engine)
    assert isinstance(spelled, list), f"the change could not be spelled: {spelled}"
    _run(connection, spelled)
    return spelled


def _a_database() -> sqlite3.Connection:
    """An in-memory SQLite database with its foreign keys enforced.

    SQLite leaves foreign keys off unless they are asked for, and a lookup
    table nothing enforces accepts every value, which is the shape that passes
    a test while refusing nothing.

    IT COMMITS AS IT GOES, so that the applier below can say where a
    transaction starts and ends. `PRAGMA foreign_keys` is a no-op inside one,
    and the driver opens one of its own unless told not to.
    """
    connection = sqlite3.connect(":memory:")
    connection.isolation_level = None
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


# --- the applier this file stands in for -------------------------------------


def _refuse_the_broken_keys(broken: list[Any]) -> None:
    """A foreign key check that found something is a failed change."""
    raise sqlite3.IntegrityError(f"foreign key check found {broken}")


def _committed(connection: sqlite3.Connection, spelled: list[Statement]) -> list[str]:
    """Run the statements in one transaction, keys checked before the commit.

    WHY THE ENFORCEMENT GOES OFF. A rebuild drops the table it is rewriting,
    and dropping a table another table points at is refused while enforcement
    is on. So the applier turns it off, rebuilds, asks the engine to check
    every key, and only then commits. A violation rolls the whole change back,
    which is how the original table survives a rebuild that should not have
    run.

    AND WHY THE RENAME IS THE LEGACY ONE. Renaming a table makes SQLite read
    the whole schema again, and between the drop and the rename another table
    is pointing at a name that is not there yet, which it refuses with `error
    in table books: no such table: main.authors`. The legacy rename renames the
    table and leaves every other table's text alone, which is what a rebuild
    wants: the name at the end is the name at the start.
    """
    connection.execute("PRAGMA foreign_keys = OFF")
    connection.execute("PRAGMA legacy_alter_table = ON")
    connection.execute("BEGIN")
    refused: list[str] = []
    try:
        _run(connection, spelled)
        broken = connection.execute("PRAGMA foreign_key_check").fetchall()
        {True: lambda: None, False: lambda: _refuse_the_broken_keys(broken)}[broken == []]()
        connection.execute("COMMIT")
    except sqlite3.Error as complaint:
        connection.execute("ROLLBACK")
        refused.append(f"{type(complaint).__name__}: {complaint}")
    connection.execute("PRAGMA legacy_alter_table = OFF")
    connection.execute("PRAGMA foreign_keys = ON")
    return refused


def _carried_out(scenario: dict, operation: Operation) -> None:
    """Spell one change for SQLite and let the applier above carry it out."""
    spelled = spell_operation(operation, "sqlite")
    scenario["answer"] = spelled
    assert isinstance(spelled, list), f"the change could not be spelled: {spelled}"
    scenario["before_schema"] = {
        name: _schema_sql(scenario["connection"], name) for name in scenario["schema"]
    }
    scenario["refused"] = _committed(scenario["connection"], spelled)


# --- what a table looks like, here and in the database -----------------------


def _rows_of(connection: sqlite3.Connection, table: str) -> list[dict[str, Value]]:
    """Every row of a table, under its column names."""
    cursor = connection.execute(f'SELECT * FROM "{table}"')
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _schema_sql(connection: sqlite3.Connection, table: str) -> str:
    """The CREATE TABLE the engine kept, which is the engine's own answer."""
    kept = connection.execute(
        'SELECT "sql" FROM "sqlite_master" WHERE "type" = ? AND "name" = ?', ("table", table)
    ).fetchall()
    return "".join(row[0] or "" for row in kept)


def _index_names(connection: sqlite3.Connection, table: str) -> list[str]:
    """The indexes a user made on a table, and not the ones SQLite made itself."""
    return [
        row[0]
        for row in connection.execute(
            'SELECT "name" FROM "sqlite_master" WHERE "type" = ? AND "tbl_name" = ?',
            ("index", table),
        ).fetchall()
        if not row[0].startswith("sqlite_autoindex")
    ]


def _keys_of(connection: sqlite3.Connection, table: str) -> set[tuple[str, str, str]]:
    """Every foreign key on a table, as child column, parent table, parent column."""
    return {
        (row[3], row[2], row[4])
        for row in connection.execute(f'PRAGMA foreign_key_list("{table}")').fetchall()
    }


def _declared_keys(scenario: dict) -> set[tuple[str, str, str]]:
    """Every foreign key the scenario's tables declare, in the same shape."""
    return {
        (column["name"], column["references"]["table"], column["references"]["column"])
        for table in scenario["schema"].values()
        for column in table["columns"]
        if column["references"] is not None
    }


def _a_value_for(column: Column) -> Value:
    """Something a column of this declared type will take."""
    return {"integer": 9001, "float": 1.5, "boolean": 1}.get(column["declared_type"].lower(), "x")


# --- what a scenario establishes: the enum and stamp tables ------------------


@given(
    parsers.re(
        r'a table "(?P<table>[^"]+)" whose status is one of "(?P<one>[^"]+)" '
        r'and "(?P<other>[^"]+)"$'
    )
)
def _a_table_whose_status_is_one_of(scenario: dict, table: str, one: str, other: str) -> None:
    """The lookup table, the table pointing at it, and the two values in it.

    Every statement here was spelled by the function under test, so a lookup
    the values never reached shows up as a scenario that cannot be set up.
    """
    connection = _a_database()
    scenario["connection"] = connection
    scenario["table"] = table

    statuses = _column("value", "text")
    status = _pointing_at(_column("status", "text"), f"{THE_STATUSES}.value", "refuse")

    _spell_and_run(
        connection,
        _operation(
            "create_table",
            THE_STATUSES,
            {},
            [statuses],
            [],
            [_rule("primary_key", "value")],
            [],
            [],
        ),
        "sqlite",
    )
    _spell_and_run(
        connection,
        _operation(
            "create_table",
            table,
            {},
            [_column("id", "integer"), status],
            [],
            [_rule("primary_key", "id")],
            [],
            [],
        ),
        "sqlite",
    )
    _spell_and_run(
        connection,
        _operation("insert_enum_values", table, {"column": "status"}, [], [], [], [], [one, other]),
        "sqlite",
    )


@given(parsers.re(r'an order whose status is "(?P<value>[^"]+)"$'))
def _an_order_whose_status_is(scenario: dict, value: str) -> None:
    connection: sqlite3.Connection = scenario["connection"]
    connection.execute(
        f'INSERT INTO "{scenario["table"]}" ("id", "status") VALUES (?, ?)', (1, value)
    )


# --- what a scenario establishes: a table this file then rebuilds ------------


@given("a database connection")
def _a_database_connection(scenario: dict) -> None:
    """An empty database, and nothing declared on it yet."""
    scenario["connection"] = _a_database()
    scenario["schema"] = {}
    scenario["refused"] = []
    scenario["answer"] = []


# WHAT A CELL OF A COLUMN TABLE MEANS. The feature writes a column as a row of a
# Gherkin table. `name` and `type` are the column; `references` is the key it
# points at; every other heading is a rule about it, and each says whether the
# rule is there rather than what it holds, except `default`, which holds one.
#
# A `nullable` OF true IS NO RULE AT ALL. A column that may be empty is a column
# nothing rules, which is why the row reads as the absence of a not_null.
_WHAT_A_HEADING_DECLARES: dict[str, Callable[[str, str], list[Constraint]]] = {
    "nullable": lambda column, cell: [_rule("not_null", column)][: int(cell.strip() == "false")],
    "primary_key": lambda column, cell: [_rule("primary_key", column)][
        : int(cell.strip() == "true")
    ],
    "unique": lambda column, cell: [_rule("unique", column)][: int(cell.strip() == "true")],
    "default": lambda column, cell: [_rule("default", column, cell.strip().strip("'"))],
}


def _a_column_from(row: dict[str, str]) -> Column:
    """One declared column, read off one row of a Gherkin table."""
    points_at = row.get("references", "").strip()
    return _pointing_at(_column(row["name"], row["type"]), points_at or None, "refuse")


def _the_rules_in(row: dict[str, str]) -> list[Constraint]:
    """Every rule one row of a Gherkin table declares about its column."""
    return [
        rule
        for heading, declares in _WHAT_A_HEADING_DECLARES.items()
        if row.get(heading, "").strip() != ""
        for rule in declares(row["name"], row[heading])
    ]


def _read(datatable: list[list[str]]) -> list[dict[str, str]]:
    """A Gherkin table as rows under its own headings."""
    headings, *rows = datatable
    return [dict(zip(headings, row, strict=True)) for row in rows]


def _with_a_key(columns: list[Column], constraints: list[Constraint]) -> list[Constraint]:
    """The same rules, with the first column made the key if none is.

    A FOREIGN KEY NEEDS A KEY TO POINT AT. Half the Gherkin tables in this
    feature write `references | authors.id` and give `authors.id` no primary
    key heading, and SQLite answers `foreign key mismatch` on the first insert,
    because a parent column with nothing unique about it is not a parent. Every
    one of those tables names its key first, so the first column is it.
    """
    declared = [rule for rule in constraints if rule["kind"] == "primary_key"]
    return {
        True: lambda: constraints,
        False: lambda: [_rule("primary_key", columns[0]["name"]), *constraints],
    }[declared != []]()


def _declared(scenario: dict, declaring: Table) -> None:
    """Write the table down and make it, with the function under test.

    IT KEEPS A COPY OF ITS OWN. A sentence that names a shape reaches the same
    `Table` every time it is said, and an index added to one scenario's table
    would otherwise be an index in the next scenario's.
    """
    table = Table(
        name=declaring["name"],
        columns=[{**column} for column in declaring["columns"]],
        indexes=[{**index} for index in declaring["indexes"]],
        constraints=[{**constraint} for constraint in declaring["constraints"]],
    )
    scenario["schema"][table["name"]] = table
    scenario["table"] = table["name"]
    # An empty table is still what the rows were before, for a scenario that
    # declares a table and changes it without putting anything in it.
    scenario["before"] = {name: [] for name in scenario["schema"]}
    _spell_and_run(
        scenario["connection"],
        _operation(
            "create_table",
            table["name"],
            {},
            table["columns"],
            table["indexes"],
            table["constraints"],
            [],
            [],
        ),
        "sqlite",
    )


# A CONDITION THAT NARROWS BY NOTHING. A key and a promise of uniqueness read
# the columns they name and never the rule, and `Constraint.rule` is a
# `Condition`, so this is the value that says the rule is not where the meaning
# is.
_NARROWS_BY_NOTHING: Condition = {"each": {}, "any_of": [], "none_of": []}


def _one_key(constraints: list[Constraint]) -> list[Constraint]:
    """The rules, with every primary key folded into the one key a table has.

    PRIMARY KEY WRITTEN TWICE IN ONE CREATE TABLE IS WHAT NO ENGINE TAKES. A
    Gherkin table marking two columns as the key means one key over both, in the
    order the table wrote them.
    """
    keys = [rule for rule in constraints if rule["kind"] == "primary_key"]
    named = [column for rule in keys for column in rule["columns"]]
    rest = [rule for rule in constraints if rule["kind"] != "primary_key"]
    folded: dict[bool, Callable[[], list[Constraint]]] = {
        False: lambda: constraints,
        True: lambda: [
            a_constraint(
                name=f"pk_{'_'.join(named)}",
                kind="primary_key",
                columns=named,
                rule=NARROWS_BY_NOTHING,
                value=None,
            ),
            *rest,
        ],
    }
    return folded[len(keys) > 1]()


@given(parsers.re(r'a table "(?P<name>[^"]+)" with columns:$'))
def _a_table_with_columns(scenario: dict, name: str, datatable: list[list[str]]) -> None:
    """One table, its columns and its rules read off the Gherkin table."""
    rows = _read(datatable)
    columns = [_a_column_from(row) for row in rows]
    declared = [rule for row in rows for rule in _the_rules_in(row)]
    _declared(
        scenario,
        Table(
            name=name,
            columns=columns,
            indexes=[],
            constraints=_one_key(_with_a_key(columns, declared)),
        ),
    )


# THE TABLES A SENTENCE NAMES WITHOUT WRITING THEM OUT. Three scenarios say
# "tables with foreign key relationships" and leave the shape to the reader, so
# the shape is a row here rather than three step functions.
_AUTHORS = Table(
    name="authors",
    columns=[_column("id", "integer"), _column("name", "text")],
    indexes=[],
    constraints=[_rule("primary_key", "id")],
)
_BOOKS = Table(
    name="books",
    columns=[
        _column("id", "integer"),
        _column("title", "text"),
        _pointing_at(_column("author_id", "integer"), "authors.id", "refuse"),
    ],
    indexes=[],
    constraints=[_rule("primary_key", "id")],
)

_TABLES_A_SENTENCE_MEANS: dict[str, list[Table]] = {
    "tables with foreign key relationships": [_AUTHORS, _BOOKS],
}


def _one_of(sentences: Any) -> Any:
    """A pattern matching exactly the sentences given, and nothing else."""
    return parsers.re("^(?P<sentence>" + "|".join(re.escape(one) for one in sentences) + ")$")


@given(_one_of(_TABLES_A_SENTENCE_MEANS))
def _the_tables_a_sentence_means(scenario: dict, sentence: str) -> None:
    """A sentence that names a shape instead of writing it out."""
    for table in _TABLES_A_SENTENCE_MEANS[sentence]:
        _declared(scenario, table)


@given(
    parsers.re(r'an? (?P<unique>unique )?index "(?P<name>[^"]+)" on column "(?P<column>[^"]+)"$')
)
def _an_index_on_a_column(scenario: dict, unique: str | None, name: str, column: str) -> None:
    """One index, written down and made by the function under test."""
    index = Index(name=name, columns=[column], unique=unique is not None)
    table = scenario["schema"][scenario["table"]]
    table["indexes"].append(index)
    _spell_and_run(
        scenario["connection"],
        _operation("create_index", table["name"], {}, [], [index], [], [], []),
        "sqlite",
    )


# --- what a scenario establishes: the rows in those tables -------------------


def _insert(connection: sqlite3.Connection, table: str, row: dict[str, Value]) -> None:
    """One row, bound, into whichever table the sentence named."""
    columns = ", ".join(f'"{name}"' for name in row)
    marks = ", ".join("?" for _ in row)
    connection.execute(f'INSERT INTO "{table}" ({columns}) VALUES ({marks})', tuple(row.values()))


def _cells(row: dict[str, str]) -> dict[str, Value]:
    """The cells a row filled in, with the word NULL meaning nothing."""
    return {
        heading: {True: None, False: cell}[cell.strip() == "NULL"]
        for heading, cell in row.items()
        if heading != "table" and cell.strip() != ""
    }


def _rows_into_one_table(scenario: dict, datatable: list[list[str]]) -> None:
    """Every row of a Gherkin table into the table the last Given named."""
    for row in _read(datatable):
        _insert(scenario["connection"], scenario["table"], _cells(row))


def _rows_into_the_table_each_names(scenario: dict, datatable: list[list[str]]) -> None:
    """Every row into the table its own first cell names."""
    for row in _read(datatable):
        _insert(scenario["connection"], row["table"], _cells(row))


def _rows_nothing_should_have_accepted(scenario: dict, datatable: list[list[str]]) -> None:
    """Rows that break a key, put in behind the engine's back.

    A scenario about a database somebody has already corrupted cannot set that
    database up while the keys are enforced, so enforcement goes off for the
    length of the insert and back on afterwards.
    """
    connection: sqlite3.Connection = scenario["connection"]
    connection.execute("PRAGMA foreign_keys = OFF")
    _rows_into_one_table(scenario, datatable)
    connection.execute("PRAGMA foreign_keys = ON")


# THE SENTENCE A SCENARIO WRITES -> HOW ITS GHERKIN TABLE IS READ.
_ROWS_ARRIVE: dict[str, Callable[[dict, list[list[str]]], None]] = {
    "the table contains data:": _rows_into_one_table,
    "the table contains hierarchical data:": _rows_into_one_table,
    "the tables contain related data:": _rows_into_the_table_each_names,
    "the books table contains invalid foreign key data:": _rows_nothing_should_have_accepted,
    "the books table has orphaned foreign keys (data corruption):": (
        _rows_nothing_should_have_accepted
    ),
}


@given(_one_of(_ROWS_ARRIVE))
def _the_rows_a_scenario_writes_out(
    scenario: dict, sentence: str, datatable: list[list[str]]
) -> None:
    """Every Given that writes its rows out as a Gherkin table."""
    _ROWS_ARRIVE[sentence](scenario, datatable)
    scenario["before"] = {
        name: _rows_of(scenario["connection"], name) for name in scenario["schema"]
    }


def _one_row_with_a_hole(scenario: dict) -> None:
    """One row whose second column is empty, for the scenarios about a copy
    that has to fail: a NULL is what a NOT NULL column refuses."""
    table = scenario["schema"][scenario["table"]]
    first, second = table["columns"][0], table["columns"][1]
    _insert(scenario["connection"], table["name"], {first["name"]: 1, second["name"]: None})


def _a_hundred_rows(scenario: dict) -> None:
    """A hundred rows, each value of a kind its own column will take."""
    table = scenario["schema"][scenario["table"]]
    for count in range(1, 101):
        _insert(
            scenario["connection"],
            table["name"],
            {
                column["name"]: {True: count, False: f"{column['name']}-{count}"}[
                    column["declared_type"].lower() == "integer"
                ]
                for column in table["columns"]
            },
        )


def _a_parent_and_a_child(scenario: dict) -> None:
    """One row in each table, the child pointing at the parent that exists."""
    for table in scenario["schema"].values():
        _insert(
            scenario["connection"],
            table["name"],
            {column["name"]: 1 for column in table["columns"]},
        )


def _no_rows_at_all(scenario: dict) -> None:
    ((count,),) = (
        scenario["connection"].execute(f'SELECT count(*) FROM "{scenario["table"]}"').fetchall()
    )
    assert count == 0, f"the scenario says the table is empty and it holds {count} rows"


# THE SENTENCES THAT DESCRIBE THEIR ROWS INSTEAD OF WRITING THEM OUT.
_ROWS_A_SENTENCE_MEANS: dict[str, Callable[[dict], None]] = {
    "the table contains data": _one_row_with_a_hole,
    "the table contains 100 rows of test data": _a_hundred_rows,
    "the tables contain related data": _a_parent_and_a_child,
    "data that satisfies the foreign key constraints": _a_parent_and_a_child,
    "the table has 0 rows": _no_rows_at_all,
}


@given(_one_of(_ROWS_A_SENTENCE_MEANS))
def _the_rows_a_sentence_means(scenario: dict, sentence: str) -> None:
    """Every Given that names a shape of data rather than writing it out."""
    _ROWS_A_SENTENCE_MEANS[sentence](scenario)
    scenario["before"] = {
        name: _rows_of(scenario["connection"], name) for name in scenario["schema"]
    }


# --- what a scenario does ----------------------------------------------------

# THE WORD A SCENARIO WRITES FOR AN ENGINE -> the engine handed to the function.
# A scenario that means an engine nobody ships says so in English rather than
# naming a database, because which unshipped database it is does not matter.
_WHICH_ENGINE: dict[str | None, str] = {
    None: "sqlite",
    "an engine this package does not ship": "mysql",
}

# THE WORDS A SCENARIO WRITES FOR A DEFAULT -> the facets the column carries.
_DEFAULTING_TO: dict[str, Callable[[str], list[Constraint]]] = {
    "nothing": lambda _column: [],
    "the number 0": lambda column: [_rule("default", column, 0)],
    "(now())": lambda column: [_rule("default", column, "(now())")],
}


@when(
    parsers.re(
        r'a column "(?P<name>[^"]+)" declared "(?P<declared>[^"]+)" defaulting to '
        r"(?P<default>nothing|the number 0|\(now\(\)\)) is added on "
        r"(?P<engine>an engine this package does not ship|[a-z]+)$"
    )
)
def _a_column_is_added(scenario: dict, name: str, declared: str, default: str, engine: str) -> None:
    """One declared column, added on one engine, and what came back kept.

    THE ANSWERS PILE UP because one scenario adds two columns and compares the
    two statements. `answer` is the last one, which is what every other Then
    reads.
    """
    scenario["engine"] = _WHICH_ENGINE.get(engine, engine)
    scenario["operation"] = _operation(
        "add_column",
        "invoices",
        {"column": name},
        [_column(name, declared)],
        [],
        _DEFAULTING_TO[default](name),
        [],
        [],
    )
    scenario["answer"] = spell_operation(scenario["operation"], scenario["engine"])
    scenario.setdefault("answers", []).append(scenario["answer"])


@when("an operation no engine table covers is spelled")
def _an_operation_no_table_covers(scenario: dict) -> None:
    """A kind the declaration's vocabulary does not carry.

    `bulldoze_table` is not in `OperationKind`, so no engine has a row for it
    and no engine ever will until somebody declares it.
    """
    scenario["operation"] = _operation("bulldoze_table", "invoices", {}, [], [], [], [], [])
    scenario["engine"] = "sqlite"
    scenario["answer"] = spell_operation(scenario["operation"], "sqlite")


def _a_rule_comparing(column: str, operator: str, value: Value) -> Condition:
    """One declared rule, as data: `Constraint.rule` is a `Condition`.

    A CHECK IS NOT A STRING ANYWHERE IN THIS PACKAGE. The scenario says the
    comparison in English, the step writes it down as the declaration's own
    type, and the engine sees the text only after `spell_operation` has written
    it.
    """
    return {
        "each": {column: {"operator": operator, "value": value, "ignore_case": False}},
        "any_of": [],
        "none_of": [],
    }


@when(
    parsers.re(
        r'a table is created with a check "(?P<name>[^"]+)" that '
        r'"(?P<column>[^"]+)" must be greater than (?P<value>-?\d+)$'
    )
)
def _a_table_is_created_with_a_check(scenario: dict, name: str, column: str, value: str) -> None:
    """A table whose one column carries a declared rule, made on a real database."""
    constraint = a_constraint(
        name=name,
        kind="check",
        columns=[column],
        rule=_a_rule_comparing(column, "gt", int(value)),
        value=None,
    )
    scenario["connection"] = _a_database()
    scenario["table"] = "ledger"
    scenario["answer"] = _spell_and_run(
        scenario["connection"],
        _operation(
            "create_table",
            "ledger",
            {},
            [_column(column, "integer")],
            [],
            [constraint, _rule("primary_key", column)],
            [],
            [],
        ),
        "sqlite",
    )


# --- every kind of rule a model may declare ----------------------------------
#
# WHAT A SCENARIO WRITES IN THE `value` COLUMN -> what the rule carries. A kind
# that carries nothing writes a dash, because an empty cell in a Gherkin table
# reads as the column being absent.
_WHAT_THE_RULE_CARRIES: dict[bool, Callable[[str], Value]] = {
    True: lambda cell: int(cell),
    False: lambda cell: {"-": None}.get(cell, cell),
}


@when(
    parsers.re(
        r'a table is created with a (?P<kind>[a-z_]+) rule on "(?P<column>[^"]+)" '
        r"allowing (?P<value>\S+)$"
    )
)
def _a_table_with_one_rule(scenario: dict, kind: str, column: str, value: str) -> None:
    """A table of one column carrying one declared rule, spelled for SQLite."""
    carried = _WHAT_THE_RULE_CARRIES[value.isdigit()](value)
    scenario["engine"] = "sqlite"
    scenario["operation"] = _operation(
        "create_table",
        "parts",
        {},
        [_column(column, "text")],
        [],
        [_rule(kind, column, carried)],
        [],
        [],
    )
    scenario["answer"] = spell_operation(scenario["operation"], "sqlite")


@when("a table is created with a rule of a kind no clause covers")
def _a_rule_of_no_kind(scenario: dict) -> None:
    """A kind nothing writes a clause for, which is refused rather than skipped."""
    scenario["engine"] = "sqlite"
    scenario["operation"] = _operation(
        "create_table",
        "parts",
        {},
        [_column("code", "text")],
        [],
        [_rule("smells_nice", "code")],
        [],
        [],
    )
    scenario["answer"] = spell_operation(scenario["operation"], "sqlite")


@when('a table is created whose "status" starts at a value no engine can be told')
def _a_default_no_engine_takes(scenario: dict) -> None:
    """A default that is not text, a number, a boolean or nothing."""
    scenario["engine"] = "sqlite"
    scenario["operation"] = _operation(
        "create_table",
        "parts",
        {},
        [_column("status", "text")],
        [],
        [_rule("default", "status", cast(Value, {"a": "dict"}))],
        [],
        [],
    )
    scenario["answer"] = spell_operation(scenario["operation"], "sqlite")


# WHAT A SENTENCE ABOUT ONE COLUMN'S RULE DECLARES. Each row is the rule the
# table is made with, so the four Givens below are one function over a table.
_THE_RULE_A_SENTENCE_DECLARES: dict[str, Callable[[str], Constraint]] = {
    "may hold 8 characters": lambda column: _rule("length", column, 8),
    "must hold something": lambda column: _rule("not_null", column),
    'starts at "new"': lambda column: _rule("default", column, "new"),
}


@given(
    parsers.re(
        r'a table "(?P<name>[^"]+)" whose "(?P<column>[^"]+)" '
        r'(?P<rule>may hold 8 characters|must hold something|starts at "new")$'
    )
)
def _a_table_whose_column_is_ruled(scenario: dict, name: str, column: str, rule: str) -> None:
    """A real table of two columns, one of them carrying the declared rule."""
    scenario["connection"] = _a_database()
    scenario["table"] = name
    scenario["schema"] = {}
    scenario["refused"] = []
    _spell_and_run(
        scenario["connection"],
        _operation(
            "create_table",
            name,
            {},
            [_column("code", "text"), _column("status", "text")],
            [],
            [_THE_RULE_A_SENTENCE_DECLARES[rule](column)],
            [],
            [],
        ),
        "sqlite",
    )


_A_LOOKUP_CHANGE: dict[str, str] = {
    "added to": "insert_enum_values",
    "taken out of": "remove_enum_values",
}


@when(
    parsers.re(
        r'"(?P<value>[^"]*)" is (?P<change>added to|taken out of) the statuses'
        r"(?: on (?P<engine>an engine this package does not ship|[a-z]+))?$"
    )
)
def _a_status_is_changed(scenario: dict, value: str, change: str, engine: str | None) -> None:
    """Add or remove one value, and run it when the scenario has a database.

    THE ENGINE IS ALLOWED TO SAY NO HERE, and what it said is kept rather than
    thrown up the stack. Taking away a value a row still holds breaks the key
    that column has into the lookup, so the engine refuses the delete, and a
    scenario about that refusal cannot reach its Then if the refusal ends the
    step.
    """
    scenario["value"] = value
    scenario["engine"] = _WHICH_ENGINE.get(engine, engine)
    operation = _operation(
        _A_LOOKUP_CHANGE[change],
        scenario.get("table", "orders"),
        {"column": "status"},
        [],
        [],
        [],
        [],
        [value],
    )
    scenario["answer"] = spell_operation(operation, scenario["engine"])
    ran: dict[bool, Callable[[], list[str]]] = {
        True: lambda: _what_the_engine_said(scenario["connection"], scenario["answer"]),
        False: lambda: [],
    }
    scenario["refused"] = ran["connection" in scenario and isinstance(scenario["answer"], list)]()


# --- what a scenario does: the changes that rebuild a table ------------------


# THE PHRASE A SCENARIO WRITES -> WHAT IT DOES TO THE TABLE IT NAMES. Every
# alter in this feature is one of these seven, and each is one line, so they are
# rows rather than seven step functions.
#
# EACH ROW TAKES THE WHOLE TABLE AND HANDS BACK THE WHOLE TABLE. Until
# 2026-09-23 a row changed one column's facets, because that is where
# nullability and defaults lived. They are `Constraint`s now, so changing one
# changes the table's rules, and only "to type" touches a column at all.
_AN_ALTERATION: dict[
    str, Callable[[list[Column], list[Constraint], str, str], tuple[list[Column], list[Constraint]]]
] = {
    "to be NOT NULL": lambda columns, rules, column, _value: (
        columns,
        _ruling(rules, column, "not_null"),
    ),
    "to be nullable": lambda columns, rules, column, _value: (
        columns,
        _without(rules, column, "not_null"),
    ),
    "nullability": lambda columns, rules, column, _value: (
        columns,
        _without(rules, column, "not_null"),
    ),
    "to type": lambda columns, rules, column, value: (
        [_only(held, column, lambda one: {**one, "declared_type": value}) for held in columns],
        rules,
    ),
    "to have default": lambda columns, rules, column, value: (
        columns,
        _ruling(rules, column, "default", value.strip("'")),
    ),
    "to have no default": lambda columns, rules, column, _value: (
        columns,
        _without(rules, column, "default"),
    ),
    "to be NOT NULL with default": lambda columns, rules, column, value: (
        columns,
        _ruling(_ruling(rules, column, "not_null"), column, "default", value.strip("'")),
    ),
}


def _only(column: Column, named: str, change: Callable[[Column], Column]) -> Column:
    """The column changed where the sentence named it, and left alone elsewhere."""
    return {True: change, False: lambda held: held}[column["name"] == named](column)


def _where(scenario: dict, name: str, table: str | None) -> tuple[str, str]:
    """Which table a sentence means, and which column of it.

    A scenario writes the column three ways: bare, `in "books"`, and
    `"authors.bio"`. The bare one means the table the last Given declared.
    """
    qualified, _, bare = name.rpartition(".")
    return (qualified or table or scenario["table"], bare)


def _rebuilt(
    scenario: dict,
    table: str,
    columns: list[Column],
    constraints: list[Constraint],
    detail: dict[str, Value],
) -> None:
    """Carry out the change that turns the live table into this shape."""
    declared = scenario["schema"][table]
    held = [column["name"] for column in declared["columns"]]
    operation = _operation(
        "alter_column",
        table,
        detail,
        columns,
        declared["indexes"],
        constraints,
        [column["name"] for column in columns if column["name"] in held],
        [],
    )
    scenario["schema"][table] = {**declared, "columns": columns, "constraints": constraints}
    scenario["altered"] = detail["column"]
    _carried_out(scenario, operation)


@when(
    parsers.re(
        r'^I alter column "(?P<name>[^"]+)"(?: in "(?P<table>[^"]+)")? '
        r"(?P<change>to be NOT NULL with default|to be NOT NULL|to be nullable|"
        r"nullability|to type|to have default|to have no default)"
        r'(?: "(?P<value>[^"]*)")?$'
    )
)
def _a_column_is_altered(
    scenario: dict, name: str, table: str | None, change: str, value: str | None
) -> None:
    """One column changed, and the whole table rebuilt around the change."""
    where, column = _where(scenario, name, table)
    scenario["default"] = (value or "").strip("'")
    declared = scenario["schema"][where]
    columns, constraints = _AN_ALTERATION[change](
        declared["columns"], declared["constraints"], column, value or ""
    )
    _rebuilt(scenario, where, columns, constraints, {"column": column})


@when(parsers.re(r'^I drop column "(?P<name>[^"]+)"$'))
def _a_column_is_dropped(scenario: dict, name: str) -> None:
    """The table without one of its columns, which SQLite reaches by rebuilding."""
    where, column = _where(scenario, name, None)
    declared = scenario["schema"][where]
    columns = [held for held in declared["columns"] if held["name"] != column]
    # A RULE GOES WITH THE COLUMN IT NAMES. A unique over a column the new table
    # has not got is a rebuild SQLite refuses, and the column is still there
    # afterwards, which reads exactly like a drop that quietly did nothing.
    rules = [rule for rule in declared["constraints"] if column not in rule["columns"]]
    operation = _operation(
        "drop_column",
        where,
        {"column": column},
        columns,
        declared["indexes"],
        rules,
        [held["name"] for held in columns],
        [],
    )
    scenario["schema"][where] = {**declared, "columns": columns, "constraints": rules}
    _carried_out(scenario, operation)


@when(
    parsers.re(
        r'^I (?P<change>add|drop) foreign key on "(?P<child>[^"]+)"'
        r'(?: referencing "(?P<parent>[^"]+)")?(?P<cascade> with ON DELETE CASCADE)?$'
    )
)
def _a_foreign_key_is_changed(
    scenario: dict, change: str, child: str, parent: str | None, cascade: str | None
) -> None:
    """A column made to point at a parent key, or made to point at nothing.

    WHAT THE SENTENCE SAYS ABOUT THE CHILD ROW. A sentence ending "with ON
    DELETE CASCADE" declares that deleting the parent takes the children with
    it, and a sentence saying nothing declares that the delete is refused, which
    is what a parent with children means when nobody writes a rule.
    """
    where, column = _where(scenario, child, None)
    pointing: dict[str, str | None] = {"add": parent, "drop": None}
    rule: dict[bool, OnDelete] = {True: "cascade", False: "refuse"}
    _rebuilt(
        scenario,
        where,
        [
            _only(
                declared,
                column,
                lambda held: _pointing_at(held, pointing[change], rule[cascade is not None]),
            )
            for declared in scenario["schema"][where]["columns"]
        ],
        scenario["schema"][where]["constraints"],
        {"column": column},
    )


def _the_first_table_loosened(scenario: dict) -> None:
    """The first table declared, its last column made nullable.

    Three scenarios ask for "a reconstruction" without saying which column, and
    the parent of a foreign key is the one worth rebuilding, because that is the
    table another table points at.
    """
    name = next(iter(scenario["schema"]))
    columns = scenario["schema"][name]["columns"]
    _rebuilt(
        scenario,
        name,
        columns,
        _without(scenario["schema"][name]["constraints"], columns[-1]["name"], "not_null"),
        {"column": columns[-1]["name"]},
    )


def _entirely_new_column_names(scenario: dict) -> None:
    """Every column renamed, so the rebuild has nothing to carry across."""
    name = scenario["table"]
    _rebuilt(
        scenario,
        name,
        [
            {**column, "name": f"fresh_{column['name']}"}
            for column in scenario["schema"][name]["columns"]
        ],
        # THE RULES FOLLOW THE COLUMNS THEY NAME. A key still naming the old
        # column is a key over a name the new table has not got, and SQLite
        # reads a name it does not know as an expression and refuses it.
        [
            {**rule, "columns": [f"fresh_{held}" for held in rule["columns"]]}
            for rule in scenario["schema"][name]["constraints"]
        ],
        {"column": scenario["schema"][name]["columns"][0]["name"]},
    )


def _a_copy_that_cannot_work(scenario: dict) -> None:
    """The second column made NOT NULL while a row holds nothing in it."""
    name = scenario["table"]
    columns = scenario["schema"][name]["columns"]
    _rebuilt(
        scenario,
        name,
        columns,
        _ruling(scenario["schema"][name]["constraints"], columns[1]["name"], "not_null"),
        {"column": columns[1]["name"]},
    )


# THE SENTENCES THAT ASK FOR A REBUILD WITHOUT NAMING A COLUMN.
_A_REBUILD_A_SENTENCE_MEANS: dict[str, Callable[[dict], None]] = {
    "I reconstruct a table with column changes": _the_first_table_loosened,
    "I reconstruct with entirely new column names": _entirely_new_column_names,
    "reconstruction fails during data copy": _a_copy_that_cannot_work,
}


@when(_one_of(_A_REBUILD_A_SENTENCE_MEANS))
def _the_rebuild_a_sentence_means(scenario: dict, sentence: str) -> None:
    """Every When that asks for a rebuild by describing it."""
    _A_REBUILD_A_SENTENCE_MEANS[sentence](scenario)


# --- what has to be true -----------------------------------------------------


def _spelled(scenario: dict) -> list[Statement]:
    """What came back, when what came back was statements rather than a refusal."""
    answer = scenario["answer"]
    assert isinstance(answer, list), f"the change could not be spelled: {answer}"
    return answer


@then("the column arrives, and the table it lands on is never dropped")
def _the_column_arrives_and_the_table_stays(scenario: dict) -> None:
    """PostgreSQL alters a table in place, so the rows it holds are never moved."""
    column = scenario["operation"]["detail"]["column"]
    text = [statement["text"] for statement in _spelled(scenario)]
    assert [line for line in text if f'ADD COLUMN "{column}"' in line] != [], (
        f"nothing here adds {column!r} to the table: {text}"
    )
    assert [line for line in text if "DROP TABLE" in line] == [], (
        f"the table and every row in it goes, to add one column on {scenario['engine']}: {text}"
    )


@then(parsers.re(r"the statement carries the text (?P<fragment>.+)$"))
def _the_statement_carries_the_text(scenario: dict, fragment: str) -> None:
    """Every scenario that reads one fragment out of what was spelled."""
    text = " ".join(statement["text"] for statement in _spelled(scenario))
    assert fragment in text, f"the statement reads {text!r}, and the scenario asks for {fragment!r}"


@then("the two statements are the same")
def _the_two_statements_are_the_same(scenario: dict) -> None:
    """A column declared uuid and a column declared text are one column here."""
    first, second = scenario["answers"]
    assert [one["text"] for one in first] == [one["text"] for one in second], (
        f"SQLite stores both in the same storage class, and they were spelled {first} and {second}"
    )


@then("a fault comes back naming the operation and the engine")
def _a_fault_names_the_operation_and_the_engine(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == A_FAULT, f"a refusal is a Fault, returned and never raised: {answer}"
    said = answer["what_went_wrong"] + " " + answer["what_to_do_instead"]
    missing = [
        word for word in (scenario["operation"]["kind"], scenario["engine"]) if word not in said
    ]
    assert missing == [], (
        f"the fault leaves the caller to work out what was refused: {missing} missing from {said!r}"
    )


@then("the statement writes it as that engine's own boolean")
def _the_statement_writes_that_engines_boolean(scenario: dict) -> None:
    text = _spelled(scenario)[0]["text"]
    assert "boolean" in text, (
        f"{scenario['engine']} has a boolean of its own and the statement does not name it: {text}"
    )

    # AND IT IS THIS ENGINE'S. SQLite has no boolean and keeps one in an
    # INTEGER, so a spelling that reached both engines would be wrong on one.
    elsewhere = spell_operation(
        _operation(
            "add_column",
            "invoices",
            {"column": "settled"},
            [_column("settled", "boolean")],
            [],
            [],
            [],
            [],
        ),
        "sqlite",
    )
    assert isinstance(elsewhere, list) and elsewhere[0]["text"] != text, (
        f"sqlite and {scenario['engine']} got the same spelling: {text}"
    )


# --- what a real engine does with a rule it was told -------------------------

# WHAT A SENTENCE ABOUT A ROW MEANS -> the row it writes. A row naming only
# `code` is what asks whether a default reached the column.
_THE_ROW_A_SENTENCE_WRITES: dict[str, tuple[str, tuple[Value, ...]]] = {
    'naming only "code"': ("INSERT INTO parts (code) VALUES (?)", ("c1",)),
    'naming "code" and a "status" of "shipped"': (
        "INSERT INTO parts (code, status) VALUES (?, ?)",
        ("c1", "shipped"),
    ),
}


@when(parsers.re(r"a row is written (?P<what>naming .+)$"))
def _a_row_is_written(scenario: dict, what: str) -> None:
    text, values = _THE_ROW_A_SENTENCE_WRITES[what]
    scenario["connection"].execute(text, values)


@then(parsers.re(r'the row\'s "(?P<column>[^"]+)" reads "(?P<value>[^"]*)"$'))
def _the_rows_column_reads(scenario: dict, column: str, value: str) -> None:
    held = scenario["connection"].execute(f'SELECT "{column}" FROM parts').fetchall()
    assert held == [(value,)], (
        f'the row\'s "{column}" reads {held!r}, and the scenario asks for {value!r}'
    )


# WHAT A SENTENCE ABOUT ONE ROW MEANS -> the row, and whether the engine takes
# it. Each is one line, so the six are a table rather than six step functions.
_A_ROW_A_SENTENCE_MEANS: dict[str, tuple[str, tuple[Value, ...]]] = {
    'a row whose "code" is 8 characters': (
        "INSERT INTO parts (code) VALUES (?)",
        ("12345678",),
    ),
    'a row whose "code" is 9 characters': (
        "INSERT INTO parts (code) VALUES (?)",
        ("123456789",),
    ),
    'a row that leaves "code" empty': ("INSERT INTO parts (status) VALUES (?)", ("x",)),
}


@then(parsers.re(r"the engine (?P<verdict>accepts|refuses) (?P<row>a row .+)$"))
def _the_engine_answers_a_row(scenario: dict, verdict: str, row: str) -> None:
    """Write the row and read the engine's own answer, whichever way it went."""
    text, values = _A_ROW_A_SENTENCE_MEANS[row]
    refused: list[str] = []
    try:
        scenario["connection"].execute(text, values)
    except sqlite3.IntegrityError as complaint:
        refused.append(str(complaint))
    assert (refused == []) == (verdict == "accepts"), (
        f"the scenario says the engine {verdict} {row}, and the engine said {refused or 'yes'}"
    )


@then("it is refused, naming the kinds it writes")
def _it_is_refused_naming_the_kinds(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == A_FAULT, f"a refusal is a Fault, returned and never raised: {answer}"
    said = answer["what_went_wrong"] + " " + answer["what_to_do_instead"]
    unnamed = [kind for kind in ("check", "primary_key", "unique", "length") if kind not in said]
    assert unnamed == [], (
        f"the refusal does not say which kinds this package writes, so a caller "
        f"has to go and find out: {unnamed} missing from {said!r}"
    )


@then("it is refused, saying what a default may be")
def _it_is_refused_saying_what_a_default_may_be(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == A_FAULT, f"a refusal is a Fault, returned and never raised: {answer}"
    said = answer["what_to_do_instead"]
    assert "text" in said and "number" in said, (
        f"the refusal does not say what a default may be instead: {said!r}"
    )


@then("it is refused, naming the engines it has")
def _it_is_refused_naming_the_engines(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == A_FAULT, f"a refusal is a Fault, returned and never raised: {answer}"
    said = answer["what_went_wrong"] + " " + answer["what_to_do_instead"]
    unnamed = [engine for engine in THE_ENGINES if engine not in said]
    assert unnamed == [], (
        f"the refusal does not say which engines exist, so a caller who wrote "
        f"mysql has to go and find out: {unnamed} missing from {said!r}"
    )


@then(parsers.re(r'the CREATE TABLE names the check "(?P<name>[^"]+)"$'))
def _the_create_table_names_the_check(scenario: dict, name: str) -> None:
    """A rule the engine can report by name is a rule somebody can find again."""
    text = " ".join(statement["text"] for statement in _spelled(scenario))
    assert f'CONSTRAINT "{name}" CHECK (' in text, (
        f"the table was created as {text!r}, and the scenario declares a check called {name!r}"
    )


@then(parsers.re(r'the engine refuses a row whose "(?P<column>[^"]+)" is (?P<value>-?\d+)$'))
def _the_engine_refuses_a_row(scenario: dict, column: str, value: str) -> None:
    """A rule the engine only carries in its text enforces nothing, so ask it."""
    refused: list[str] = []
    try:
        _insert(scenario["connection"], scenario["table"], {column: int(value)})
    except sqlite3.IntegrityError as complaint:
        refused.append(str(complaint))
    assert refused != [], (
        f"the engine stored {value} in {column!r}, so the check reached the text and nothing else"
    )


@then(parsers.re(r'a row with status "(?P<value>[^"]+)" is (?P<verdict>accepted|refused)$'))
def _a_row_with_a_status(scenario: dict, value: str, verdict: str) -> None:
    """Insert one row and let the database say.

    The lookup is a table with a foreign key pointing at it, so a value nobody
    put in the lookup is refused by the engine rather than by this library.
    """
    connection: sqlite3.Connection = scenario["connection"]
    inserting = f'INSERT INTO "{scenario["table"]}" ("id", "status") VALUES (?, ?)'
    refused: list[str] = []
    try:
        connection.execute(inserting, (7, value))
    except sqlite3.IntegrityError as complaint:
        refused.append(str(complaint))

    said = {"accepted": [], "refused": refused or ["nothing"]}[verdict]
    # What the lookup change came back as, when the scenario made one. A change
    # nobody could spell is the reason a row is still accepted.
    spelled = [scenario[key] for key in filter(scenario.__contains__, ["answer"])]
    assert refused == said, (
        f"the engine was asked to store {value!r} and answered {refused or 'by storing it'}, "
        f"where the scenario says the row is {verdict}. The change was spelled "
        f"as: {spelled}"
    )


@then("the engine refuses, because a row still points at it")
def _the_engine_refuses_because_a_row_points_at_it(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), (
        f"taking a value out of the lookup could not be spelled: {answer}"
    )
    connection: sqlite3.Connection = scenario["connection"]
    refused: list[str] = []
    try:
        _run(connection, answer)
    except sqlite3.IntegrityError as complaint:
        refused.append(str(complaint))

    assert refused != [], (
        "a row still carries the value, so the engine has to refuse the delete; "
        "it went through, which means nothing was enforcing the lookup"
    )


@then(parsers.re(r'the value is marked "(?P<mark>[^"]+)" and travels beside the statement$'))
def _the_value_is_marked_and_travels_beside_it(scenario: dict, mark: str) -> None:
    """The value never reaches the text, so there is nothing in it to escape."""
    (statement,) = _spelled(scenario)
    assert mark in statement["text"], (
        f"{scenario['engine']} reads {mark!r} where a value goes, and the "
        f"statement reads {statement['text']!r}"
    )
    assert _values(statement) == (scenario["value"],), (
        f"the value has to travel beside the statement, and {statement} carries "
        f"{_values(statement)}"
    )


# --- what has to be true of a rebuilt table ----------------------------------


# WHAT A SENTENCE CLAIMS ABOUT ONE COLUMN'S DEFINITION -> the text the engine's
# own CREATE TABLE has to carry, given the word the sentence quoted.
_THE_DEFINITION_SAYS: dict[str, Callable[[str], str]] = {
    "as NOT NULL": lambda _value: "NOT NULL",
    "is now NOT NULL": lambda _value: "NOT NULL",
    "as type": lambda value: value,
    "with default": lambda value: f"DEFAULT {value}",
}

# The two sentences that claim a definition says NOTHING.
_THE_DEFINITION_DOES_NOT_SAY: dict[str, Callable[[str], str]] = {
    "as nullable": lambda _value: "NOT NULL",
    "with no default": lambda _value: "DEFAULT",
}


def _definition_of(scenario: dict, table: str, column: str) -> str:
    """The one clause of the engine's CREATE TABLE that defines this column."""
    sql = _schema_sql(scenario["connection"], table)
    body = sql.partition("(")[2].rpartition(")")[0]
    named = [
        clause.strip() for clause in body.split(",") if clause.strip().startswith(f'"{column}"')
    ]
    assert named != [], f"{column!r} is not in the table the engine kept: {sql!r}"
    return named[0]


@then(
    parsers.re(
        r'^(?:the table schema shows|the column) "(?P<column>[^"]+)" '
        r"(?P<claim>as NOT NULL|as nullable|is now NOT NULL|as type|with default|"
        r"with no default)"
        r"(?: \"(?P<value>[^\"]*)\")?$"
    )
)
def _the_definition_of_a_column(scenario: dict, column: str, claim: str, value: str | None) -> None:
    """Every sentence about what one column's definition says, or does not."""
    where, name = _where(scenario, column, None)
    definition = _definition_of(scenario, where, name)
    wanted = {**_THE_DEFINITION_SAYS, **_THE_DEFINITION_DOES_NOT_SAY}[claim](value or "")
    carries = wanted.upper() in definition.upper()
    assert carries is (claim in _THE_DEFINITION_SAYS), (
        f"the engine kept {definition!r} for {name!r}, and the scenario says {claim} {value or ''}"
    )


@then(parsers.re(r'^the UNIQUE constraint on "(?P<column>[^"]+)" is preserved$'))
def _the_unique_constraint_is_preserved(scenario: dict, column: str) -> None:
    """A rebuild that loses the rule loses what the rule enforced.

    IT READS THE WHOLE CREATE TABLE RATHER THAN ONE COLUMN'S CLAUSE. A unique
    is a `Constraint` over the columns it names, so the engine keeps it beside
    the columns rather than inside one of their definitions.
    """
    kept = _schema_sql(scenario["connection"], scenario["table"])
    assert "UNIQUE" in kept.upper() and column in kept, (
        f"the rebuild kept {kept!r}, and the scenario says {column!r} is still unique"
    )


@then(parsers.re(r'^the column "(?P<column>[^"]+)" does not exist$'))
def _the_column_does_not_exist(scenario: dict, column: str) -> None:
    where, name = _where(scenario, column, None)
    held = [
        row[1] for row in scenario["connection"].execute(f'PRAGMA table_info("{where}")').fetchall()
    ]
    assert name not in held, f"{where!r} still holds {held}, and the scenario drops {name!r}"


# HOW MANY ROWS A SENTENCE SAYS THE TABLE ENDS WITH. An empty table says none,
# so the count it names is nothing at all and the two numbers are one group.
_HOW_MANY_ROWS = (
    r"^(?:the table has (?P<held>\d+) rows|all (?P<kept>\d+) rows are preserved"
    r"|the table remains empty)$"
)


@then(parsers.re(_HOW_MANY_ROWS))
def _how_many_rows_are_left(scenario: dict, held: str | None, kept: str | None) -> None:
    """Every sentence that counts the rows a rebuild left behind."""
    ((count,),) = (
        scenario["connection"].execute(f'SELECT count(*) FROM "{scenario["table"]}"').fetchall()
    )
    assert count == int(held or kept or 0), (
        f"the table holds {count} rows and the scenario says {held or kept or 0}"
    )


# THE SENTENCES THAT SAY THE ROWS CAME THROUGH UNCHANGED. A type change rewrites
# `1` as `'1'`, which the scenario that asked for it calls conversion, so the
# comparison is on what each value reads as and not on its storage class.
_NOTHING_WAS_LOST = (
    "all existing data is preserved",
    "all other data is preserved",
    "the original data is intact",
    "all hierarchical relationships are intact",
)


@then(_one_of(_NOTHING_WAS_LOST))
def _nothing_was_lost(scenario: dict, sentence: str) -> None:
    """Every column that survived holds what it held before the change."""
    for table, before in scenario["before"].items():
        now = _rows_of(scenario["connection"], table)
        surviving = [column["name"] for column in scenario["schema"][table]["columns"]]
        assert [{n: f"{v}" for n, v in row.items() if n in surviving} for row in before] == [
            {n: f"{v}" for n, v in row.items() if n in surviving} for row in now
        ], f"{sentence!r}, and {table!r} went from {before} to {now}"


# WHAT `typeof` ANSWERS FOR THE TYPE A SCENARIO NAMES.
_STORED_AS: dict[str, str] = {"TEXT": "text", "INTEGER": "integer"}


@then(parsers.re(r"^the data values are converted to (?P<type>TEXT|INTEGER)$"))
def _the_values_are_converted(scenario: dict, type: str) -> None:
    """The engine's own answer for what it is now storing in that column."""
    column = scenario["altered"]
    kinds = {
        row[0]
        for row in scenario["connection"]
        .execute(f'SELECT typeof("{column}") FROM "{scenario["table"]}"')
        .fetchall()
    }
    assert kinds == {_STORED_AS[type]}, (
        f"the column was rebuilt as {type} and the engine stores {kinds}"
    )


@then("new inserts use the default value")
def _new_inserts_use_the_default(scenario: dict) -> None:
    """A row that says nothing about the column comes back carrying the default."""
    table = scenario["schema"][scenario["table"]]
    altered = scenario["altered"]
    _insert(
        scenario["connection"],
        table["name"],
        {
            column["name"]: _a_value_for(column)
            for column in table["columns"]
            if column["name"] != altered
        },
    )
    stored = [row[altered] for row in _rows_of(scenario["connection"], table["name"])]
    assert stored[-1] == scenario["default"], (
        f"the row came back carrying {stored[-1]!r} where the default is {scenario['default']!r}"
    )


# THE SENTENCES THAT SAY THE CHANGE WAS REFUSED BY THE ENGINE.
_THE_CHANGE_WAS_REFUSED = (
    "the operation fails with constraint violation",
    "the operation fails with foreign key violation",
    "foreign key check fails after reconstruction",
)


@then(_one_of(_THE_CHANGE_WAS_REFUSED))
def _the_change_was_refused(scenario: dict, sentence: str) -> None:
    """The engine refused the change, which is what the applier rolls back on."""
    assert scenario["refused"] != [], (
        f"{sentence!r}, and the engine carried the change out without complaint. "
        f"It was spelled as {[statement['text'] for statement in scenario['answer']]}"
    )


# THE SENTENCES THAT SAY THE CHANGE LEFT NOTHING BEHIND IT. A refused change is
# rolled back by whoever owns the COMMIT, and what a caller sees afterwards is
# the table it had before, which is what these ask the engine for.
_NOTHING_CHANGED = (
    "the original table is unchanged",
    "the original schema is unchanged",
)


@then(_one_of(_NOTHING_CHANGED))
def _nothing_changed(scenario: dict, sentence: str) -> None:
    """The engine's own CREATE TABLE is the one it had before the change."""
    now = {name: _schema_sql(scenario["connection"], name) for name in scenario["before_schema"]}
    assert now == scenario["before_schema"], (
        f"{sentence!r}, and the engine now keeps {now} where it kept {scenario['before_schema']}"
    )


# --- what has to be true of the keys and the indexes -------------------------

# THE SENTENCES THAT SAY THE KEYS ARE WHAT THE SCHEMA DECLARES. A key that was
# preserved, a key that was added and a key that was dropped are all this: the
# engine holds exactly the keys the tables declare afterwards.
_THE_KEYS_MATCH = (
    "the foreign key relationship is preserved",
    "the foreign key relationship exists",
    "the foreign key relationship does not exist",
    'the foreign key on "department_id" is preserved',
    'the incoming foreign key from "books" is preserved',
    "the self-referential foreign key is preserved",
)


@then(_one_of(_THE_KEYS_MATCH))
def _the_keys_are_what_the_schema_declares(scenario: dict, sentence: str) -> None:
    """Every foreign key the engine holds, against every one declared."""
    held = {key for table in scenario["schema"] for key in _keys_of(scenario["connection"], table)}
    assert held == _declared_keys(scenario), (
        f"{sentence!r}, and the engine holds {sorted(held)} where the tables "
        f"declare {sorted(_declared_keys(scenario))}"
    )


# THE SENTENCES THAT SAY A KEY IS STILL ENFORCED -> whether a child row pointing
# at a parent that does not exist is refused.
_A_BOGUS_CHILD_IS: dict[str, bool] = {
    "foreign key constraints are still enforced": True,
    "foreign key constraints are enforced": True,
    "the foreign key relationships still work": True,
    "the foreign key constraint still works": True,
    "inserting invalid author_id raises foreign key error": True,
    "inserting invalid author_id succeeds (no FK check)": False,
}


# THE CHILD A SCENARIO MEANS WHEN NO COLUMN POINTS ANYWHERE. One scenario drops
# the key and then asks whether the same row goes in, so the column it names
# survives the drop that made nothing point at it.
_THE_CHILD_WHEN_NOTHING_POINTS = ("books", "author_id")


@then(_one_of(_A_BOGUS_CHILD_IS))
def _a_child_row_with_no_parent(scenario: dict, sentence: str) -> None:
    """Insert a row pointing at a parent nothing has, and let the engine say."""
    pointing = [
        (table["name"], column["name"])
        for table in scenario["schema"].values()
        for column in table["columns"]
        if column["references"] is not None
    ]
    where, child = {True: lambda: pointing[0], False: lambda: _THE_CHILD_WHEN_NOTHING_POINTS}[
        pointing != []
    ]()
    row: dict[str, Value] = {
        column["name"]: _a_value_for(column) for column in scenario["schema"][where]["columns"]
    }
    refused: list[str] = []
    try:
        _insert(scenario["connection"], where, {**row, child: 987654})
    except sqlite3.Error as complaint:
        refused.append(str(complaint))
    assert (refused != []) is _A_BOGUS_CHILD_IS[sentence], (
        f"{sentence!r}, and the engine answered {refused or 'by storing the row'}"
    )


# THE SENTENCES THAT ASK THE ENGINE WHETHER ANY KEY IS BROKEN.
_NO_KEY_IS_BROKEN = ("foreign key checks pass after reconstruction",)


@then(_one_of(_NO_KEY_IS_BROKEN))
def _no_key_is_broken(scenario: dict, sentence: str) -> None:
    """`PRAGMA foreign_key_check` is the engine's own answer, not ours."""
    broken = scenario["connection"].execute("PRAGMA foreign_key_check").fetchall()
    assert broken == [], f"{sentence!r}, and the engine reports {broken}"


@then(
    parsers.re(
        r'^(?:the (?:unique )?index "(?P<name>[^"]+)" still exists|both indexes are functional)$'
    )
)
def _an_index_survived_the_rebuild(scenario: dict, name: str | None) -> None:
    """An index belongs to its table, so a rebuild that forgets one loses it."""
    held = _index_names(scenario["connection"], scenario["table"])
    wanted = (
        [index["name"] for index in scenario["schema"][scenario["table"]]["indexes"]],
        [name],
    )[name is not None]
    assert [one for one in wanted if one not in held] == [], (
        f"the rebuilt table carries {held} and the scenario asks for {wanted}"
    )


# THE SENTENCES THAT SAY THE REBUILD ITSELF CAME THROUGH.
_THE_TABLE_IS_BACK = (
    "the table is recreated",
    "the table is reconstructed successfully",
)


@then(_one_of(_THE_TABLE_IS_BACK))
def _the_table_is_back(scenario: dict, sentence: str) -> None:
    """The change was not refused and the table is there under its own name."""
    assert scenario["refused"] == [], f"{sentence!r}, and the engine said {scenario['refused']}"
    assert _schema_sql(scenario["connection"], scenario["table"]) != "", (
        f"{sentence!r}, and the engine has no table called {scenario['table']!r}"
    )


@then("no data is copied (no common columns)")
def _no_data_is_copied(scenario: dict) -> None:
    """A rebuild with nothing on both sides spells no INSERT at all."""
    copying = [
        statement["text"] for statement in scenario["answer"] if "INSERT INTO" in statement["text"]
    ]
    assert copying == [], f"the rebuild carries rows across with {copying}"


@then("the change came back spelled, not refused")
def _the_change_came_back_spelled(scenario: dict) -> None:
    """A rebuild that carries no row across is still a rebuild.

    THIS IS WHERE A WARNING WOULD HAVE GONE. Nothing in this package logs: a
    refusal comes back in the return, so a caller who wants to know whether the
    rows survived reads the answer rather than a file somewhere.
    """
    answer = scenario["answer"]
    assert isinstance(answer, list), (
        f"a rebuild that carries no row across is still a rebuild, and this came "
        f"back refused: {answer}"
    )


@then("deleting category cascades to products")
def _deleting_the_parent_cascades(scenario: dict) -> None:
    """ON DELETE CASCADE, which `Column.references` cannot say."""
    scenario["connection"].execute('DELETE FROM "categories" WHERE "id" = 1')
    left = _rows_of(scenario["connection"], "products")
    assert left == [], f"the child rows are still there: {left}"


@then("the composite primary key is preserved")
@then("both columns remain part of primary key")
def _the_composite_primary_key_is_preserved(scenario: dict) -> None:
    """A table-level PRIMARY KEY, which no field of an `Operation` can carry."""
    keys = [
        row[1]
        for row in scenario["connection"]
        .execute(f'PRAGMA table_info("{scenario["table"]}")')
        .fetchall()
        if row[5] > 0
    ]
    assert len(keys) > 1, f"the rebuilt table has {keys} as its primary key"
