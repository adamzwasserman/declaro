"""Bind `tests/bdd/features/schema/schema_inspect.feature`.

WHAT THE FUNCTION ANSWERS. `fn schema_inspect` is handed a database and says
what that database currently holds: its tables, each table's columns, the keys
those columns point along, the indexes over them and the rules declared on them.
It is the only place this library asks a database what is there, and it asks so
`fn difference` can compare, never so anything can decide what belongs.

EVERY SCENARIO HERE OPENS A REAL DATABASE. A stand-in that gets a driver
contract wrong has manufactured a defect in this package twice: once inventing a
pyturso bug that was not there, and once being more permissive than the driver
and hiding a regression. So each scenario creates tables with the engine's own
DDL, asks the function, and checks the answer describes what was made.

WHICH ENGINE EACH SCENARIO RUNS ON. Six of them make a local SQLite database,
because they ask about the shape of the answer rather than about an engine. The
last one is a Scenario Outline over all three, and it is the one that asks
whether what comes back is what the engine really holds. PostgreSQL is reached
through the database this pytest process made for itself, in a schema of this
file's own that it drops afterwards, and a server that is not running is a
failure rather than a skip: `tests/conftest.py` records the run where 186
passing tests went quiet as skips and nobody counted the `s` characters. Turso
is pyturso, opened on a local file, which is Turso with no primary. It is not
libsql and not libsql-experimental; those are other projects.

THE IDENTIFIERS ARE ALL DIFFERENT ON PURPOSE. This package once reached full
branch coverage while emitting SQL no engine could parse, because every test
named its table `users`. So no two scenarios here name the same table, and one
of them names a table with a double quote inside it, which is the character that
has to be doubled on the way into a pragma.

THE FUNCTION IS ASYNCHRONOUS AND EVERY STEP HERE IS NOT. pytest-bdd does not
await what a step returns, so an `async def` step leaves a coroutine nobody ran
and the scenario asserts on an untouched context. Each scenario is given a loop
of its own by a fixture that closes it, and the steps drive the function on that
loop, which is also what lets one asyncpg connection live from the Given to the
Then.

NOTHING HERE READS THE MODULE'S OWN TEXT. Every claim below is about a value
that came back from the function or from an engine.
"""

from __future__ import annotations

import asyncio
import pathlib
import sqlite3
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.schema_from_declared import schema_from_declared
from declaro_persistum.schema_inspect import schema_inspect
from declaro_persistum.types import Column, Database, Declared, Model, Schema
from tests.conftest import POSTGRESQL_PROBE_URL, refuse_without_postgresql

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/schema/schema_inspect.feature")


A_FAULT = {"kind", "what_went_wrong", "what_to_do_instead"}
A_SCHEMA = {"tables", "views"}

# The PostgreSQL scenario works inside a schema of this file's own, so nothing
# it makes is visible to another test and nothing another test made is visible
# to it.
_A_SCHEMA_OF_OUR_OWN = "schema_inspect_probe"


# =============================================================================
# A loop of this scenario's own, and everything it opened closed afterwards
# =============================================================================


@pytest.fixture
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    """One event loop per scenario, closed by whoever made it.

    A loop nobody closes is finalised whenever the collector next runs, and
    pytest attributes the `ValueError: Invalid file descriptor: -1` to whichever
    unrelated test was running then.
    """
    made = asyncio.new_event_loop()
    try:
        yield made
    finally:
        made.close()


@pytest.fixture(autouse=True)
def _everything_opened_is_closed(scenario: dict, loop: asyncio.AbstractEventLoop) -> Iterator[None]:
    """Close every connection a step opened and drop every table it made.

    It is set up after the loop and torn down before it, so a closer that has
    to run a statement still has a loop to run it on.
    """
    yield
    for close in scenario.get("closing", []):
        close()


# =============================================================================
# The databases a scenario is given
# =============================================================================
#
# ONE FUNCTION OVER A TABLE, because six sentences ask for six databases and
# differ in nothing but which statements build them. Each row is the DDL the
# scenario's database is made out of, under the words the scenario says.

_THE_DATABASES: dict[str, tuple[str, ...]] = {
    "holding one table with two columns": (
        "CREATE TABLE gazette (gazette_id integer PRIMARY KEY, headline text NOT NULL)",
    ),
    "with no tables": (),
    "holding a table whose column points at another table": (
        "CREATE TABLE harbour (harbour_id integer PRIMARY KEY, berths integer)",
        "CREATE TABLE vessel (vessel_id integer PRIMARY KEY, moored_at integer "
        "REFERENCES harbour(harbour_id) ON DELETE CASCADE)",
    ),
    "holding a table with a declared rule on one column": (
        "CREATE TABLE cargo (cargo_id integer PRIMARY KEY, tonnage integer, "
        "CONSTRAINT tonnage_is_positive CHECK (tonnage > 0))",
    ),
    "holding a table with an index over one of its columns": (
        "CREATE TABLE lighthouse (lighthouse_id integer PRIMARY KEY, lamp_range integer)",
        "CREATE INDEX lighthouse_by_range ON lighthouse (lamp_range)",
    ),
    "holding a table whose name carries a double quote": (
        'CREATE TABLE "The ""Mary Rose""" (wreck_id integer PRIMARY KEY, sank_in integer)',
    ),
}

# The table the scenario about adoption finds and no model declares, beside the
# one table a model does declare.
_UNDECLARED = (
    "CREATE TABLE ledger (ledger_id integer PRIMARY KEY, balance integer)",
    "CREATE TABLE driftwood (driftwood_id integer PRIMARY KEY, found_on text)",
)

# The table every engine is asked about: three columns, an index over one of
# them, and a key pointing at another table.
_THREE_COLUMNS = (
    "CREATE TABLE anchorage (anchorage_id integer PRIMARY KEY, depth integer)",
    "CREATE TABLE sounding (sounding_id integer PRIMARY KEY, taken_at text, "
    "anchorage integer REFERENCES anchorage(anchorage_id) ON DELETE CASCADE)",
    "CREATE INDEX sounding_by_time ON sounding (taken_at)",
)


def _sqlite(
    scenario: dict, _loop: asyncio.AbstractEventLoop, made: pathlib.Path
) -> tuple[Any, Callable[[tuple[str, ...]], None]]:
    """A SQLite database in a file this scenario owns."""
    connection = sqlite3.connect(made / "inspected.db")
    scenario.setdefault("closing", []).append(connection.close)

    def run(statements: tuple[str, ...]) -> None:
        for statement in statements:
            connection.execute(statement)
        connection.commit()

    return connection, run


def _turso(
    scenario: dict, _loop: asyncio.AbstractEventLoop, made: pathlib.Path
) -> tuple[Any, Callable[[tuple[str, ...]], None]]:
    """A Turso database with no primary, through pyturso."""
    try:
        import turso
    except ImportError as missing:
        pytest.fail(f"Turso is a supported backend and pyturso is not installed: {missing}")

    connection = turso.connect(str(made / "inspected.db"))
    scenario.setdefault("closing", []).append(connection.close)

    def run(statements: tuple[str, ...]) -> None:
        for statement in statements:
            connection.execute(statement)

    return connection, run


def _postgresql(
    scenario: dict, loop: asyncio.AbstractEventLoop, _made: pathlib.Path
) -> tuple[Any, Callable[[tuple[str, ...]], None]]:
    """PostgreSQL, in a schema of this file's own inside this process's database."""
    import asyncpg

    try:
        connection = loop.run_until_complete(asyncpg.connect(POSTGRESQL_PROBE_URL, timeout=5))
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(POSTGRESQL_PROBE_URL, unreachable)

    def run(statements: tuple[str, ...]) -> None:
        for statement in statements:
            loop.run_until_complete(connection.execute(statement))

    def close() -> None:
        loop.run_until_complete(
            connection.execute(f'DROP SCHEMA IF EXISTS "{_A_SCHEMA_OF_OUR_OWN}" CASCADE')
        )
        loop.run_until_complete(connection.close())

    scenario.setdefault("closing", []).append(close)
    run(
        (
            f'DROP SCHEMA IF EXISTS "{_A_SCHEMA_OF_OUR_OWN}" CASCADE',
            f'CREATE SCHEMA "{_A_SCHEMA_OF_OUR_OWN}"',
            f'SET search_path TO "{_A_SCHEMA_OF_OUR_OWN}"',
        )
    )
    return connection, run


# ONE ROW PER ENGINE, read by subscript. A word with no row raises at the lookup
# rather than being served by whichever row happens to be first.
_OPENING: dict[str, Callable[..., tuple[Any, Callable[[tuple[str, ...]], None]]]] = {
    "sqlite": _sqlite,
    "turso": _turso,
    "postgresql": _postgresql,
}


def _held_by(
    scenario: dict,
    loop: asyncio.AbstractEventLoop,
    made: pathlib.Path,
    engine: str,
    statements: tuple[str, ...],
) -> Database:
    """Open one engine, build the tables, and hand back what a caller keeps."""
    connection, run = _OPENING[engine](scenario, loop, made)
    run(statements)
    return Database(
        engine=engine,  # type: ignore[typeddict-item]
        connection=connection,
        writers=[],
        room={"arrivals": [], "waiting": []},
        shutdown="exit_immediately",
        primary=None,
    )


@given(parsers.re(r"^a database (?P<held>.+)$"))
def _a_database_holding(
    scenario: dict, loop: asyncio.AbstractEventLoop, tmp_path: pathlib.Path, held: str
) -> None:
    """Six sentences, six databases, one function over the table above."""
    scenario["db"] = _held_by(scenario, loop, tmp_path, "sqlite", _THE_DATABASES[held])


@given(
    parsers.re(
        r"^a table on (?P<engine>\w+) with three columns, an index and a key "
        r"pointing at another table$"
    )
)
def _a_table_on_an_engine(
    scenario: dict, loop: asyncio.AbstractEventLoop, tmp_path: pathlib.Path, engine: str
) -> None:
    """The same three tables on whichever of the three engines is named."""
    scenario["db"] = _held_by(scenario, loop, tmp_path, engine, _THREE_COLUMNS)


# =============================================================================
# Asking
# =============================================================================


@when("it is inspected")
@when("the database is inspected")
def _it_is_inspected(scenario: dict, loop: asyncio.AbstractEventLoop) -> None:
    """One function for both sentences, because both ask the same question."""
    scenario["answer"] = loop.run_until_complete(schema_inspect(scenario["db"]))


def _a_model_of(table: str, columns: list[str]) -> Model:
    """One model declaring a table and its columns, and promising nothing else."""
    return Model(
        table=table,
        columns=[
            Column(
                name=name,
                declared_type="integer",
                renamed_from=None,
                references=None,
                facets={},
            )
            for name in columns
        ],
        dropping=[],
        indexes=[],
        constraints=[],
    )


@when("a database holds a table no model declares")
def _a_database_holds_a_table_no_model_declares(
    scenario: dict, loop: asyncio.AbstractEventLoop, tmp_path: pathlib.Path
) -> None:
    """Two tables in the database, one model, and both answers read side by side.

    The live schema is what the database holds. The target schema is what the
    models say should be there, and it is built from the models alone.
    """
    scenario["db"] = _held_by(scenario, loop, tmp_path, "sqlite", _UNDECLARED)
    scenario["live"] = loop.run_until_complete(schema_inspect(scenario["db"]))
    declared = Declared(
        models=[_a_model_of("ledger", ["ledger_id", "balance"])],
        views=[],
        dropping_tables=[],
        dropping_views=[],
    )
    scenario["target"] = schema_from_declared(declared)


# =============================================================================
# What came back
# =============================================================================


def _a_schema(answer: Any) -> Schema:
    """The answer as a schema, or a failure naming what came back instead."""
    assert set(answer) == A_SCHEMA, f"a schema was asked for and this came back: {answer}"
    return answer


def _named(schema: Schema, name: str) -> Any:
    """The one table of that name, or a failure listing the names there are."""
    found = [table for table in schema["tables"] if table["name"] == name]
    assert len(found) == 1, (
        f"{name} is not one table in the answer; it holds "
        f"{[table['name'] for table in schema['tables']]}"
    )
    return found[0]


@then("the table and both columns come back")
def _the_table_and_both_columns_come_back(scenario: dict) -> None:
    schema = _a_schema(scenario["answer"])
    table = _named(schema, "gazette")

    assert [column["name"] for column in table["columns"]] == ["gazette_id", "headline"], table
    assert [column["declared_type"] for column in table["columns"]] == ["integer", "text"], table


@then("an empty schema comes back, and that is not a failure")
def _an_empty_schema_comes_back(scenario: dict) -> None:
    """An empty database is an answer. A `Fault` here would make every first
    open look like a broken one."""
    answer = scenario["answer"]

    assert set(answer) != A_FAULT, f"an empty database was read as a failure: {answer}"
    assert _a_schema(answer)["tables"] == [], answer


@then("the table is not adopted into the target schema")
def _the_table_is_not_adopted(scenario: dict) -> None:
    """The database holds it and the models do not, so the target has not got it.

    Adopting it would make a column somebody added by hand in production part
    of the specification, and nothing would ever drop it.
    """
    live = [table["name"] for table in _a_schema(scenario["live"])["tables"]]
    target = [table["name"] for table in _a_schema(scenario["target"])["tables"]]

    assert "driftwood" in live, f"the database holds driftwood and the read missed it: {live}"
    assert "driftwood" not in target, f"a table no model declares reached the target: {target}"
    assert target == ["ledger"], f"the target is what the models declare and nothing else: {target}"


@then("the column comes back naming that table, that column, and what a delete does to the child")
def _the_column_comes_back_naming_where_it_points(scenario: dict) -> None:
    """A reference is three facts travelling together, and the third is the one
    a string like `harbour.harbour_id` could never carry."""
    table = _named(_a_schema(scenario["answer"]), "vessel")
    pointing = [column for column in table["columns"] if column["references"] is not None]

    assert len(pointing) == 1, f"one column points at another table, and these did: {table}"
    reference = pointing[0]["references"]
    assert pointing[0]["name"] == "moored_at", pointing[0]
    assert reference["table"] == "harbour", reference
    assert reference["column"] == "harbour_id", reference
    assert reference["on_delete"] == "cascade", reference


@then("the rule comes back as a condition rather than as a string")
def _the_rule_comes_back_as_a_condition(scenario: dict) -> None:
    """A CHECK the engine keeps only in the text it stored still has to reach
    the caller, and it reaches it as data."""
    table = _named(_a_schema(scenario["answer"]), "cargo")

    assert len(table["constraints"]) == 1, f"one rule was declared and this came back: {table}"
    rule = table["constraints"][0]
    assert rule["name"] == "tonnage_is_positive", rule
    assert rule["kind"] == "check", rule
    assert rule["columns"] == ["tonnage"], rule
    assert not isinstance(rule["rule"], str), f"the rule came back as words: {rule}"
    assert rule["rule"]["each"] == {
        "tonnage": {"operator": "gt", "value": 0, "ignore_case": False}
    }, rule


@then("the index comes back under its declared name, naming that column")
def _the_index_comes_back_under_its_name(scenario: dict) -> None:
    table = _named(_a_schema(scenario["answer"]), "lighthouse")

    assert [index["name"] for index in table["indexes"]] == ["lighthouse_by_range"], table
    assert table["indexes"][0]["columns"] == ["lamp_range"], table["indexes"][0]
    assert table["indexes"][0]["unique"] is False, table["indexes"][0]


@then("that table and its columns come back, and nothing was refused")
def _the_quoted_table_comes_back(scenario: dict) -> None:
    """The name carries the character a pragma has to have doubled on the way
    in. Get that wrong and the name comes back broken, or nothing comes back."""
    answer = scenario["answer"]

    assert set(answer) != A_FAULT, f"a name with a quote in it was refused: {answer}"
    table = _named(_a_schema(answer), 'The "Mary Rose"')
    assert [column["name"] for column in table["columns"]] == ["wreck_id", "sank_in"], table


@then("all three columns come back, with the index and the key the table declares")
def _all_three_columns_with_the_index_and_the_key(scenario: dict) -> None:
    """One claim per thing the table declares, asked of whichever engine holds it."""
    schema = _a_schema(scenario["answer"])
    table = _named(schema, "sounding")

    assert [column["name"] for column in table["columns"]] == [
        "sounding_id",
        "taken_at",
        "anchorage",
    ], table
    assert "sounding_by_time" in [index["name"] for index in table["indexes"]], table["indexes"]
    covered = [
        index["columns"] for index in table["indexes"] if index["name"] == "sounding_by_time"
    ]
    assert covered == [["taken_at"]], covered

    pointing = [column for column in table["columns"] if column["references"] is not None]
    assert [column["name"] for column in pointing] == ["anchorage"], table
    assert pointing[0]["references"]["table"] == "anchorage", pointing[0]
    assert pointing[0]["references"]["column"] == "anchorage_id", pointing[0]
    assert pointing[0]["references"]["on_delete"] == "cascade", pointing[0]
