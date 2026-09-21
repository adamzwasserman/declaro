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

WHAT IS NOT SATISFIED, and is failing rather than faked. Each of these is a
disagreement between this feature and `fn spell_operation` as the declaration
writes it, so each needs a decision rather than more code.

A COMPOSITE PRIMARY KEY. Two columns each marked `primary_key` become two
inline PRIMARY KEY clauses, which no engine takes. A table-level
`PRIMARY KEY (a, b)` needs a `Constraint` that is not a CHECK, and
`Constraint.rule` is a `Condition`, which is comparisons. One scenario asks for
it and dies in its own Given, naming the SQL.

A RULE ABOUT WHAT HAPPENS TO A CHILD ROW. `Column.references` carries the
`table.column` a column points at and nothing else, so ON DELETE CASCADE has
nowhere to sit. One scenario asks for it.

A WARNING IN A LOG. This package returns faults and logs nothing, by rule. One
scenario asks for a warning to be logged when a rebuild copies no rows.

THE PRAGMA ITSELF. One scenario asks that foreign keys be temporarily disabled
by the change. They are disabled by whoever commits, for the reason above, so
no statement that comes back can say it.

A COPY THAT FAILS ON A TYPE. One scenario stores `invalid` in a TEXT column and
asks that converting it to INTEGER fail during the copy. SQLite has type
affinity rather than type enforcement and stores the text unchanged, so the
copy succeeds.

THE TWO QUOTE-DOUBLING ROWS. They ask for the enum value to reach the statement
TEXT with its quote doubled. An enum value is bound and travels beside the
statement, which is what `Statement.parameters` is for, so the text carries a
placeholder and the value never reaches it. Doubling a quote is what you do
when you have pasted, and this does not paste.

TAKING A VALUE OUT OF THE LOOKUP. `OperationKind` has `insert_enum_values` and
no word for removing one, so there is no operation to spell. Two scenarios ask
for it.

THE RANDOM TAIL ON A TEMPORARY NAME. Two scenarios want two names for one table
to differ, which needs a random source, and `fn spell_operation` is pure. The
rebuild drops a leftover temporary table before it makes one instead, so a
retry after a crash needs no tail to tell it apart.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.spell_operation import (
    TEMPORARY_PREFIX,
    column_definition,
    constraint_clause,
    default_clause,
    listing_the_user_tables,
    placeholder,
    same_type,
    spell_operation,
    stamping_the_schema_hash,
    temporary_name,
    type_spelled,
)
from declaro_persistum.types import Column, Engine, Index, Operation, Statement, Table, Value

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/schema/spell_operation.feature")

A_FAULT = {"kind", "what_went_wrong", "what_to_do_instead"}
THE_ENGINES = ("sqlite", "postgresql", "turso")

# The lookup table a declared `Literal` becomes, for the table and column the
# enum scenarios use.
THE_STATUSES = "_dp_enum_orders_status"


# --- the pieces a scenario hands over ---------------------------------------


def _column(name: str, declared_type: str, **facets: Value) -> Column:
    """One declared column, with every field a `Column` carries."""
    return {
        "name": name,
        "declared_type": declared_type,
        "renamed_from": None,
        "references": None,
        "facets": dict(facets),
    }


def _pointing_at(column: Column, parent: str | None) -> Column:
    """The same column, pointing at a parent key or at nothing."""
    return {**column, "references": parent}


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
        (column["name"], *str(column["references"]).rpartition(".")[::2])
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

    statuses = _column("value", "text", primary_key=True)
    status = _pointing_at(_column("status", "text"), f"{THE_STATUSES}.value")

    _spell_and_run(
        connection,
        _operation("create_table", THE_STATUSES, {}, [statuses], [], [], [], []),
        "sqlite",
    )
    _spell_and_run(
        connection,
        _operation(
            "create_table",
            table,
            {},
            [_column("id", "integer", primary_key=True), status],
            [],
            [],
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


@given("a stamp table on a real database")
def _a_stamp_table(scenario: dict) -> None:
    connection = _a_database()
    scenario["connection"] = connection
    _spell_and_run(
        connection,
        _operation(
            "create_table",
            "_declaro_meta",
            {},
            [_column("key", "text", primary_key=True), _column("value", "text")],
            [],
            [],
            [],
            [],
        ),
        "sqlite",
    )


@given(parsers.re(r'a table "(?P<table>[^"]+)" that the user made$'))
def _a_table_the_user_made(scenario: dict, table: str) -> None:
    _spell_and_run(
        scenario["connection"],
        _operation(
            "create_table",
            table,
            {},
            [_column("id", "integer", primary_key=True)],
            [],
            [],
            [],
            [],
        ),
        "sqlite",
    )
    scenario["table"] = table


# --- what a scenario establishes: a table this file then rebuilds ------------


@given("a database connection")
def _a_database_connection(scenario: dict) -> None:
    """An empty database, and nothing declared on it yet."""
    scenario["connection"] = _a_database()
    scenario["schema"] = {}
    scenario["refused"] = []
    scenario["answer"] = []


# WHAT A CELL OF A COLUMN TABLE MEANS. The feature writes a column as a row of
# a Gherkin table, and each heading is one thing a `Column` carries. `type` is
# the declared type; the rest are facets, except `references`, which is the key
# the column points at.
_A_FACET: dict[str, Callable[[str], Value]] = {
    "nullable": lambda cell: cell.strip().lower() == "true",
    "primary_key": lambda cell: cell.strip().lower() == "true",
    "unique": lambda cell: cell.strip().lower() == "true",
    "default": lambda cell: cell.strip().strip("'"),
}


def _a_column_from(row: dict[str, str]) -> Column:
    """One declared column, read off one row of a Gherkin table."""
    column = _column(
        row["name"],
        row["type"],
        **{
            heading: _A_FACET[heading](row[heading])
            for heading in _A_FACET
            if row.get(heading, "").strip() != ""
        },
    )
    points_at = row.get("references", "").strip()
    return _pointing_at(column, points_at or None)


def _read(datatable: list[list[str]]) -> list[dict[str, str]]:
    """A Gherkin table as rows under its own headings."""
    headings, *rows = datatable
    return [dict(zip(headings, row, strict=True)) for row in rows]


def _with_a_key(columns: list[Column]) -> list[Column]:
    """The same columns, with the first one made the key if none is.

    A FOREIGN KEY NEEDS A KEY TO POINT AT. Half the Gherkin tables in this
    feature write `references | authors.id` and give `authors.id` no primary
    key heading, and SQLite answers `foreign key mismatch` on the first insert,
    because a parent column with nothing unique about it is not a parent. Every
    one of those tables names its key first, so the first column is it.
    """
    declared = [column for column in columns if column["facets"].get("primary_key") is True]
    return {
        True: lambda: columns,
        False: lambda: [
            {**columns[0], "facets": {**columns[0]["facets"], "primary_key": True}},
            *columns[1:],
        ],
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


@given(parsers.re(r'a table "(?P<name>[^"]+)" with columns:$'))
def _a_table_with_columns(scenario: dict, name: str, datatable: list[list[str]]) -> None:
    """One table, its columns read off the Gherkin table under the sentence."""
    _declared(
        scenario,
        Table(
            name=name,
            columns=_with_a_key([_a_column_from(row) for row in _read(datatable)]),
            indexes=[],
            constraints=[],
        ),
    )


# THE TABLES A SENTENCE NAMES WITHOUT WRITING THEM OUT. Three scenarios say
# "tables with foreign key relationships" and leave the shape to the reader, so
# the shape is a row here rather than three step functions.
_AUTHORS = Table(
    name="authors",
    columns=[_column("id", "integer", primary_key=True), _column("name", "text", nullable=True)],
    indexes=[],
    constraints=[],
)
_BOOKS = Table(
    name="books",
    columns=[
        _column("id", "integer", primary_key=True),
        _column("title", "text", nullable=True),
        _pointing_at(_column("author_id", "integer", nullable=True), "authors.id"),
    ],
    indexes=[],
    constraints=[],
)
_NEEDS_REBUILDING = Table(
    name="widgets",
    columns=[_column("id", "integer", primary_key=True), _column("label", "text", nullable=True)],
    indexes=[],
    constraints=[],
)

_TABLES_A_SENTENCE_MEANS: dict[str, list[Table]] = {
    "tables with foreign key relationships": [_AUTHORS, _BOOKS],
    "a table requiring reconstruction": [_NEEDS_REBUILDING],
}


def _one_of(sentences: Any) -> Any:
    """A pattern matching exactly the sentences given, and nothing else."""
    return parsers.re("^(?P<sentence>" + "|".join(re.escape(one) for one in sentences) + ")$")


@given(_one_of(_TABLES_A_SENTENCE_MEANS))
def _the_tables_a_sentence_means(scenario: dict, sentence: str) -> None:
    """A sentence that names a shape instead of writing it out."""
    for table in _TABLES_A_SENTENCE_MEANS[sentence]:
        _declared(scenario, table)


@given("foreign keys are enabled")
def _foreign_keys_are_enabled(scenario: dict) -> None:
    """The database this file opens enforces them, and says so when asked."""
    ((enforced,),) = scenario["connection"].execute("PRAGMA foreign_keys").fetchall()
    assert enforced == 1, "a lookup nothing enforces accepts every value"


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
    "the table contains data with incompatible types:": _rows_into_one_table,
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

# WHICH CHANGE EACH PHRASE STANDS FOR. Two scenarios differ by the change and by
# the engine, so the words are read and the change is a row.
_A_CHANGE: dict[str, Operation] = {
    "adding a column": _operation(
        "add_column",
        "invoices",
        {"column": "settled_on"},
        [_column("settled_on", "date", nullable=True)],
        [],
        [],
        [],
        [],
    ),
    "dropping a column": _operation(
        "drop_column",
        "invoices",
        {"column": "memo"},
        [_column("id", "integer"), _column("settled_on", "date", nullable=True)],
        [],
        [],
        ["id", "settled_on"],
        [],
    ),
}


@when(
    parsers.re(r"(?P<change>adding a column|dropping a column) is spelled for (?P<engine>[a-z]+)$")
)
def _a_change_is_spelled_for(scenario: dict, change: str, engine: str) -> None:
    scenario["operation"] = _A_CHANGE[change]
    scenario["engine"] = engine
    scenario["answer"] = spell_operation(_A_CHANGE[change], engine)


@when("an operation no engine table covers is spelled")
def _an_operation_no_table_covers(scenario: dict) -> None:
    """A kind the declaration's vocabulary does not carry.

    `bulldoze_table` is not in `OperationKind`, so no engine has a row for it
    and no engine ever will until somebody declares it.
    """
    scenario["operation"] = _operation("bulldoze_table", "invoices", {}, [], [], [], [], [])
    scenario["engine"] = "sqlite"
    scenario["answer"] = spell_operation(scenario["operation"], "sqlite")


# The word a scenario writes for a speller -> the engine it is spelling for.
_WHOSE_SPELLING: dict[str, Engine] = {
    "sqlite_type": "sqlite",
    "postgresql_type": "postgresql",
    "sqlite_default": "sqlite",
    "postgresql_default": "postgresql",
}


@when(parsers.re(r'(?P<speller>sqlite_type|postgresql_type) is given "(?P<declared>[^"]*)"$'))
def _a_type_is_spelled(scenario: dict, speller: str, declared: str) -> None:
    scenario["answer"] = type_spelled(declared, _WHOSE_SPELLING[speller])


@when(
    parsers.re(
        r"(?P<speller>sqlite_default|postgresql_default) is given "
        r'(?:the number (?P<number>-?\d+)|"(?P<text>[^"]*)")$'
    )
)
def _a_default_is_spelled(
    scenario: dict, speller: str, number: str | None, text: str | None
) -> None:
    """One step for both spellers, over the two ways a scenario writes a value."""
    written: dict[bool, Callable[[], Value]] = {
        True: lambda: int(number or "0"),
        False: lambda: text,
    }
    scenario["answer"] = default_clause(written[number is not None](), _WHOSE_SPELLING[speller])


@when(
    parsers.re(
        r'same_type compares "(?P<declared>[^"]*)" and "(?P<live>[^"]*)" '
        r'on "(?P<engine>[^"]*)"$'
    )
)
def _two_spellings_are_compared(scenario: dict, declared: str, live: str, engine: str) -> None:
    scenario["answer"] = same_type(declared, live, engine)


@when(
    parsers.re(
        r'column_definition is given a text column "(?P<name>[^"]+)" '
        r'for "(?P<engine>[^"]*)"$'
    )
)
def _a_column_is_defined(scenario: dict, name: str, engine: str) -> None:
    scenario["answer"] = column_definition(_column(name, "text"), engine)


@when(
    parsers.re(
        r'constraint_clause is given a check "(?P<name>[^"]+)" on '
        r'"(?P<column>\w+) > (?P<value>-?\d+)"$'
    )
)
def _a_constraint_is_written(scenario: dict, name: str, column: str, value: str) -> None:
    scenario["name"] = name
    scenario["answer"] = constraint_clause(
        {
            "name": name,
            "columns": [column],
            "rule": {
                "each": {column: {"operator": "gt", "value": int(value), "ignore_case": False}},
                "any_of": [],
                "none_of": [],
            },
        },
        "sqlite",
    )


@when(parsers.re(r"a boolean column is spelled for (?P<engine>[a-z]+)$"))
def _a_boolean_column_is_spelled(scenario: dict, engine: str) -> None:
    scenario["engine"] = engine
    scenario["answer"] = spell_operation(
        _operation(
            "add_column",
            "invoices",
            {"column": "settled"},
            [_column("settled", "boolean", nullable=False)],
            [],
            [],
            [],
            [],
        ),
        engine,
    )


@when("a column is spelled for an engine this package does not ship")
def _a_column_is_spelled_for_an_engine_we_do_not_ship(scenario: dict) -> None:
    scenario["answer"] = column_definition(_column("settled_on", "date"), "mysql")


_A_LOOKUP_CHANGE: dict[str, str] = {
    "added to": "insert_enum_values",
    "taken out of": "remove_enum_values",
}


@when(parsers.re(r'"(?P<value>[^"]*)" is (?P<change>added to|taken out of) the statuses$'))
def _a_status_is_changed(scenario: dict, value: str, change: str) -> None:
    """Add or remove one value, and run it when the scenario has a database.

    `remove_enum_values` is not a word `OperationKind` carries. The step asks
    for it by the name the change would have, so the fault names the gap rather
    than this file inventing a spelling nobody declared.
    """
    scenario["value"] = value
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
    scenario["answer"] = spell_operation(operation, "sqlite")
    ran: dict[bool, Callable[[], None]] = {
        True: lambda: _run(scenario["connection"], scenario["answer"]),
        False: lambda: None,
    }
    ran["connection" in scenario and isinstance(scenario["answer"], list)]()


_A_MIGRATION_STATEMENT: dict[str, Callable[[Engine], Any]] = {
    "stamping the schema's hash": lambda engine: stamping_the_schema_hash(
        engine, "declaro_todo", "9f2c"
    ),
    "listing the user's tables": listing_the_user_tables,
}


@when(
    parsers.re(
        r"(?P<what>stamping the schema's hash|listing the user's tables) is spelled for "
        r"(?:(?P<engine>[a-z]+)|an engine this package does not ship)$"
    )
)
def _a_migration_statement_is_spelled(scenario: dict, what: str, engine: str | None) -> None:
    scenario["answer"] = _A_MIGRATION_STATEMENT[what](engine or "mysql")


@when(parsers.re(r'"_placeholder" is asked for "(?P<engine>[^"]*)"$'))
def _the_placeholder_is_asked_for(scenario: dict, engine: str) -> None:
    scenario["answer"] = placeholder(engine)


_HOW_MANY_NAMES: dict[str, int] = {"a": 1, "two": 2}


@when(parsers.re(r'(?P<count>a|two) temporary names? (?:is|are) made for "(?P<table>[^"]+)"$'))
def _temporary_names_are_made(scenario: dict, count: str, table: str) -> None:
    scenario["table"] = table
    scenario["names"] = [temporary_name(table) for _ in range(_HOW_MANY_NAMES[count])]
    scenario["answer"] = scenario["names"][0]


@when("the same key is stamped twice with different values")
def _the_same_key_is_stamped_twice(scenario: dict) -> None:
    connection: sqlite3.Connection = scenario["connection"]
    for value in ("9f2c", "a10d"):
        statement = stamping_the_schema_hash("sqlite", "declaro_todo", value)
        assert isinstance(statement, dict) and "text" in statement, statement
        _run(connection, [statement])
    scenario["value"] = "a10d"


@when("the user-tables query is run")
def _the_user_tables_query_is_run(scenario: dict) -> None:
    statement = listing_the_user_tables("sqlite")
    assert isinstance(statement, dict) and "text" in statement, statement
    scenario["rows"] = [
        row[0]
        for row in scenario["connection"].execute(statement["text"], _values(statement)).fetchall()
    ]


# --- what a scenario does: the changes that rebuild a table ------------------


def _facet(column: Column, facet: str, value: Value) -> Column:
    return {**column, "facets": {**column["facets"], facet: value}}


def _without_facet(column: Column, facet: str) -> Column:
    return {**column, "facets": {n: v for n, v in column["facets"].items() if n != facet}}


# THE PHRASE A SCENARIO WRITES -> WHAT IT DOES TO THE COLUMN IT NAMES. Every
# alter in this feature is one of these seven, and each is one line, so they are
# rows rather than seven step functions.
_AN_ALTERATION: dict[str, Callable[[Column, str], Column]] = {
    "to be NOT NULL": lambda column, _value: _facet(column, "nullable", False),
    "to be nullable": lambda column, _value: _facet(column, "nullable", True),
    "nullability": lambda column, _value: _facet(column, "nullable", True),
    "to type": lambda column, value: {**column, "declared_type": value},
    "to have default": lambda column, value: _facet(column, "default", value.strip("'")),
    "to have no default": lambda column, _value: _without_facet(column, "default"),
    "to be NOT NULL with default": lambda column, value: _facet(
        _facet(column, "nullable", False), "default", value.strip("'")
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


def _rebuilt(scenario: dict, table: str, columns: list[Column], detail: dict[str, Value]) -> None:
    """Carry out the change that turns the live table into this shape."""
    declared = scenario["schema"][table]
    held = [column["name"] for column in declared["columns"]]
    operation = _operation(
        "alter_column",
        table,
        detail,
        columns,
        declared["indexes"],
        declared["constraints"],
        [column["name"] for column in columns if column["name"] in held],
        [],
    )
    scenario["schema"][table] = {**declared, "columns": columns}
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
    _rebuilt(
        scenario,
        where,
        [
            _only(declared, column, lambda held: _AN_ALTERATION[change](held, value or ""))
            for declared in scenario["schema"][where]["columns"]
        ],
        {"column": column},
    )


@when(parsers.re(r'^I drop column "(?P<name>[^"]+)"$'))
def _a_column_is_dropped(scenario: dict, name: str) -> None:
    """The table without one of its columns, which SQLite reaches by rebuilding.

    IT IS ALSO SPELLED FOR THE ENGINE THAT DROPS IN PLACE, because one scenario
    asks whether a simple drop can be a single ALTER TABLE. The rows are read
    on the engine this file can run.
    """
    where, column = _where(scenario, name, None)
    declared = scenario["schema"][where]
    columns = [held for held in declared["columns"] if held["name"] != column]
    operation = _operation(
        "drop_column",
        where,
        {"column": column},
        columns,
        declared["indexes"],
        declared["constraints"],
        [held["name"] for held in columns],
        [],
    )
    scenario["elsewhere"] = spell_operation(operation, "postgresql")
    scenario["schema"][where] = {**declared, "columns": columns}
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
    """A column made to point at a parent key, or made to point at nothing."""
    where, column = _where(scenario, child, None)
    pointing: dict[str, str | None] = {"add": parent, "drop": None}
    _rebuilt(
        scenario,
        where,
        [
            _only(declared, column, lambda held: _pointing_at(held, pointing[change]))
            for declared in scenario["schema"][where]["columns"]
        ],
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
        columns[:-1] + [_facet(columns[-1], "nullable", True)],
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
        {"column": scenario["schema"][name]["columns"][0]["name"]},
    )


def _a_copy_that_cannot_work(scenario: dict) -> None:
    """The second column made NOT NULL while a row holds nothing in it."""
    name = scenario["table"]
    columns = scenario["schema"][name]["columns"]
    _rebuilt(
        scenario,
        name,
        [columns[0], _facet(columns[1], "nullable", False), *columns[2:]],
        {"column": columns[1]["name"]},
    )


# THE SENTENCES THAT ASK FOR A REBUILD WITHOUT NAMING A COLUMN.
_A_REBUILD_A_SENTENCE_MEANS: dict[str, Callable[[dict], None]] = {
    "I reconstruct a table with column changes": _the_first_table_loosened,
    "I perform table reconstruction": _the_first_table_loosened,
    "I reconstruct with entirely new column names": _entirely_new_column_names,
    "reconstruction fails during data copy": _a_copy_that_cannot_work,
}


@when(_one_of(_A_REBUILD_A_SENTENCE_MEANS))
def _the_rebuild_a_sentence_means(scenario: dict, sentence: str) -> None:
    """Every When that asks for a rebuild by describing it."""
    _A_REBUILD_A_SENTENCE_MEANS[sentence](scenario)


# --- what has to be true -----------------------------------------------------


@then("one statement comes back")
def _one_statement_comes_back(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), f"a spelled change comes back as a list: {answer}"
    assert len(answer) == 1, (
        f"adding a column is one ALTER TABLE on {scenario['engine']}, and "
        f"{len(answer)} statements came back: {answer}"
    )


@then("several statements come back, and they rebuild the table")
def _several_statements_rebuild_the_table(scenario: dict) -> None:
    """The four moves of a rebuild, in the order they have to run.

    SQLite cannot drop a column out of a table that carries constraints, so the
    change is a new table, the rows copied into it, the old one gone and the new
    one renamed into its place.
    """
    answer = scenario["answer"]
    assert isinstance(answer, list), f"a spelled change comes back as a list: {answer}"
    table = scenario["operation"]["table"]
    temporary = temporary_name(table)
    text = [statement["text"] for statement in answer]

    moves = [
        f'CREATE TABLE "{temporary}"',
        f'INSERT INTO "{temporary}"',
        f'DROP TABLE "{table}"',
        f'ALTER TABLE "{temporary}" RENAME TO "{table}"',
    ]
    found = [
        next((position for position, line in enumerate(text) if move in line), -1) for move in moves
    ]

    assert -1 not in found, (
        "a rebuild makes a new table, copies the rows into it, drops the old "
        f"one and renames the new one; these moves are missing: "
        f"{[move for move, at in zip(moves, found, strict=True) if at == -1]}\n{text}"
    )
    assert found == sorted(found), (
        f"the rebuild's statements came back in an order that cannot run: {text}"
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


@then(parsers.re(r'it (?:answers|gives back) "(?P<expected>[^"]*)"$'))
def _it_answers(scenario: dict, expected: str) -> None:
    """Every scenario that reads one quoted answer, whichever verb it uses."""
    assert scenario["answer"] == expected, (
        f"it answered {scenario['answer']!r} where the scenario asks for {expected!r}"
    )


@then("the statement writes it as that engine's own boolean")
def _the_statement_writes_that_engines_boolean(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), f"a spelled change comes back as a list: {answer}"
    text = answer[0]["text"]
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
            [_column("settled", "boolean", nullable=False)],
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


@then("it is refused, naming the engines it has")
@then("it refuses and names the engines it has")
def _it_is_refused_naming_the_engines(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == A_FAULT, f"a refusal is a Fault, returned and never raised: {answer}"
    said = answer["what_went_wrong"] + " " + answer["what_to_do_instead"]
    unnamed = [engine for engine in THE_ENGINES if engine not in said]
    assert unnamed == [], (
        f"the refusal does not say which engines exist, so a caller who wrote "
        f"mysql has to go and find out: {unnamed} missing from {said!r}"
    )


@then("it answers that they are the same")
def _it_answers_that_they_are_the_same(scenario: dict) -> None:
    assert scenario["answer"] is True, (
        f"a declared uuid is a TEXT column on SQLite, and it answered {scenario['answer']!r}"
    )


@then('it answers "name" TEXT')
def _it_answers_a_quoted_name_and_a_type(scenario: dict) -> None:
    assert scenario["answer"] == '"name" TEXT', (
        f"a column is its quoted name and the type this engine stores it in, "
        f"and it answered {scenario['answer']!r}"
    )


@then(parsers.re(r'it answers a CHECK clause naming "(?P<name>[^"]+)"$'))
def _it_answers_a_check_clause(scenario: dict, name: str) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, str), f"a clause is text a CREATE TABLE carries: {answer}"
    assert answer.startswith(f'CONSTRAINT "{name}" CHECK ('), (
        f"a CREATE TABLE carries a constraint by name, and this reads {answer!r}"
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


@then("the statement carries the value with its quote doubled")
def _the_statement_carries_the_value_with_its_quote_doubled(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), f"the change could not be spelled: {answer}"
    doubled = scenario["value"].replace("'", "''")
    carrying = [statement["text"] for statement in answer if doubled in statement["text"]]
    assert carrying != [], (
        f"no statement carries {doubled!r}: {[statement['text'] for statement in answer]}"
    )


@then("the second value is what is stored")
def _the_second_value_is_what_is_stored(scenario: dict) -> None:
    stored = (
        scenario["connection"]
        .execute('SELECT "value" FROM "_declaro_meta" WHERE "key" = ?', ("declaro_todo",))
        .fetchall()
    )
    assert [row[0] for row in stored] == [scenario["value"]], (
        f"stamping the same key twice left {stored}, and the second stamp is the one that is true"
    )


@then("there is one row, not two")
def _there_is_one_row_not_two(scenario: dict) -> None:
    ((count,),) = scenario["connection"].execute('SELECT count(*) FROM "_declaro_meta"').fetchall()
    assert count == 1, f"the stamp is written over itself, and there are {count} rows"


@then(parsers.re(r'it names "(?P<table>[^"]+)" and not the stamp table$'))
def _it_names_the_users_table_and_not_ours(scenario: dict, table: str) -> None:
    assert scenario["rows"] == [table], (
        f"the query answers with the tables a user made, and it answered {scenario['rows']}"
    )


@then("a statement comes back")
def _a_statement_comes_back(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == {"text", "parameters"}, f"a Statement carries two things: {answer}"
    assert answer["text"].strip() != "", "a statement with no text runs nowhere"


@then("it begins with the temporary prefix")
def _it_begins_with_the_temporary_prefix(scenario: dict) -> None:
    assert scenario["answer"].startswith(TEMPORARY_PREFIX), (
        f"orphan recovery looks for {TEMPORARY_PREFIX!r} and the name is {scenario['answer']!r}"
    )


@then("it carries the table's own name")
def _it_carries_the_tables_own_name(scenario: dict) -> None:
    assert scenario["table"] in scenario["answer"], (
        f"a leftover table nobody can trace back to {scenario['table']!r} is a "
        f"table nobody dares drop: {scenario['answer']!r}"
    )


@then("they are not the same name")
def _they_are_not_the_same_name(scenario: dict) -> None:
    first, second = scenario["names"]
    assert first != second, (
        "two temporary names for one table are the same, because a pure "
        "function reads no random source. The rebuild drops a leftover "
        "temporary table before it makes one, which is what a tail was for"
    )


@then("the tail is exactly as wide as the constant says")
def _the_tail_is_as_wide_as_the_constant_says(scenario: dict) -> None:
    tail = scenario["answer"].removeprefix(TEMPORARY_PREFIX).removeprefix(scenario["table"])
    assert tail != "", (
        "the name carries no tail. A random tail needs a random source, and "
        "`fn spell_operation` is pure, so the rebuild drops a leftover "
        "temporary table instead of telling two of them apart"
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
    """A rebuild that loses a UNIQUE facet loses what the facet enforced, so
    the engine is asked to store the same value twice."""
    definition = _definition_of(scenario, scenario["table"], column)
    assert "UNIQUE" in definition.upper(), (
        f"the rebuild kept {definition!r}, and the scenario says {column!r} is still unique"
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
    "the operation fails during data copy",
    "foreign key check fails after reconstruction",
)


@then(_one_of(_THE_CHANGE_WAS_REFUSED))
def _the_change_was_refused(scenario: dict, sentence: str) -> None:
    """The engine refused the change, which is what the applier rolls back on."""
    assert scenario["refused"] != [], (
        f"{sentence!r}, and the engine carried the change out without complaint. "
        f"It was spelled as {[statement['text'] for statement in scenario['answer']]}"
    )


# THE SENTENCES THAT SAY NOTHING CHANGED, because the change was rolled back.
_NOTHING_CHANGED = (
    "the transaction is rolled back",
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


@then("the temp table is cleaned up")
def _the_temp_table_is_cleaned_up(scenario: dict) -> None:
    """A rolled-back rebuild leaves no half-built table behind."""
    left = (
        scenario["connection"]
        .execute(
            'SELECT "name" FROM "sqlite_master" WHERE "type" = ? AND "name" = ?',
            ("table", temporary_name(scenario["table"])),
        )
        .fetchall()
    )
    assert left == [], f"the rebuild left {left} behind for the next attempt to trip over"


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
_NO_KEY_IS_BROKEN = (
    "foreign key checks pass after reconstruction",
    "no foreign key violations remain after reconstruction",
    "foreign key checks are performed",
)


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


@then("only explicit CREATE INDEX statements are recreated")
def _only_declared_indexes_are_recreated(scenario: dict) -> None:
    """A UNIQUE facet makes SQLite an index of its own, and a rebuild that
    wrote that one out again would make it twice."""
    made = [
        statement["text"] for statement in scenario["answer"] if "CREATE INDEX" in statement["text"]
    ]
    declared = scenario["schema"][scenario["table"]]["indexes"]
    assert len(made) == len(declared), (
        f"the rebuild spells {made} where the table declares {[i['name'] for i in declared]}"
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


@then("deleting category cascades to products")
def _deleting_the_parent_cascades(scenario: dict) -> None:
    """ON DELETE CASCADE, which `Column.references` cannot say."""
    scenario["connection"].execute('DELETE FROM "categories" WHERE "id" = 1')
    left = _rows_of(scenario["connection"], "products")
    assert left == [], f"the child rows are still there: {left}"


# THE SENTENCES NOTHING HERE CAN SATISFY, and what each is waiting on. A step
# that asserted nothing would turn its scenario green on a body nobody wrote, so
# each one fails naming the decision it needs.
_NOBODY_CAN_ANSWER: dict[str, str] = {
    "a warning is logged": (
        "a failure comes back in the return here and is never logged, so "
        "`fn spell_operation` writes nothing anywhere"
    ),
    "foreign keys are temporarily disabled": (
        "`PRAGMA foreign_keys` is a no-op inside a transaction, so it cannot be "
        "one of the statements that come back; the applier that owns the COMMIT "
        "is what turns enforcement off"
    ),
}


@then(_one_of(_NOBODY_CAN_ANSWER))
def _a_sentence_nothing_here_satisfies(scenario: dict, sentence: str) -> None:
    """Every line this function cannot answer, failing by name."""
    pytest.fail(
        f"{sentence!r}: {_NOBODY_CAN_ANSWER[sentence]}. The line needs a decision, not more code."
    )


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


@then("the operation uses direct ALTER TABLE DROP COLUMN")
def _the_drop_is_one_statement_where_the_engine_allows_it(scenario: dict) -> None:
    """PostgreSQL drops a column in place, which is one statement and no copy."""
    elsewhere = scenario["elsewhere"]
    assert isinstance(elsewhere, list) and len(elsewhere) == 1, (
        f"a simple drop is one statement on an engine that alters in place: {elsewhere}"
    )
    assert "DROP COLUMN" in elsewhere[0]["text"], elsewhere[0]["text"]
