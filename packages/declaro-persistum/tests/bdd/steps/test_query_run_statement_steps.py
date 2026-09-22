"""Bind `tests/bdd/features/query/query_run_statement.feature`.

WHAT THE FEATURE ASKS FOR. Four results about the one call that reaches a
database: a select comes back as rows, a migration's create table runs on the
connection the database holds rather than on a drainer's, a statement the
engine refuses comes back as a fault carrying what the engine said, and a value
that reads like the end of a statement is stored character for character.

EVERY SCENARIO RUNS ON ALL THREE ENGINES, INSIDE THE SCENARIO. The feature says
nothing about engines, because the result is the same on each; `boundary_out fn
query_run_statement` is one door with a row per driver behind it, and a row
nobody runs is a row nobody has seen work. So each step opens SQLite,
PostgreSQL and Turso, runs the same work on each, and asserts the same result
for each.

NOTHING HERE IS A STAND-IN. A double that got a driver contract wrong has
manufactured a defect in this package twice: once inventing a pyturso bug that
was not there, once being more permissive than the driver and hiding a real
regression. SQLite is `aiosqlite`, PostgreSQL is `asyncpg` on the probe
database this pytest process made, and Turso is `pyturso`, opened on a file of
its own. PostgreSQL's absence fails rather than skips, through
`require_postgresql`, because 186 passing tests once went quiet as skips when
the server was down.

NO SQL IS WRITTEN HERE. `fn compile_statement` writes the selects and the
insert and `fn spell_operation` writes the create table, each for the engine it
is about to run on, which is also what puts the driver's own placeholders in
the text and the caller's value beside it.

THE DRAINER IN EVERY SCENARIO IS A CLOSED CONNECTION. That is what makes "not
on a drainer's" an assertion rather than a hope: a create table that had gone
to `writers` would have failed on a connection the driver has already closed,
and it comes back with a count instead.
"""

from __future__ import annotations

import asyncio
import pathlib
import tempfile
import uuid
from collections.abc import Awaitable, Callable

from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.compile_statement import compile_statement
from declaro_persistum.query_run_statement import query_run_statement
from declaro_persistum.spell_operation import spell_operation
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/query/query_run_statement.feature")


# --- the three engines, opened for real --------------------------------------
#
# ONE ROW PER ENGINE, THREE ENTRIES EACH: how a connection is opened, how it is
# closed, and how a statement is put to the driver with nothing catching what it
# raises. The third is the oracle for the refusal: the engine's own words are
# whatever the driver says when the same text goes to it directly.


async def _open_sqlite(folder: pathlib.Path):
    import aiosqlite

    return await aiosqlite.connect(folder / "sqlite.db")


async def _close_sqlite(connection) -> None:
    await connection.close()


async def _run_on_sqlite(connection, text: str) -> None:
    await connection.execute(text)


async def _open_postgresql(_folder: pathlib.Path):
    import asyncpg

    return await asyncpg.connect(POSTGRESQL_PROBE_URL)


async def _close_postgresql(connection) -> None:
    await connection.close()


async def _run_on_postgresql(connection, text: str) -> None:
    await connection.fetch(text)


async def _open_turso(folder: pathlib.Path):
    import turso

    return turso.connect(str(folder / "turso.db"))


async def _close_turso(connection) -> None:
    connection.close()


async def _run_on_turso(connection, text: str) -> None:
    connection.execute(text)


THE_THREE_ENGINES = {
    "sqlite": {"open": _open_sqlite, "close": _close_sqlite, "run": _run_on_sqlite},
    "postgresql": {"open": _open_postgresql, "close": _close_postgresql, "run": _run_on_postgresql},
    "turso": {"open": _open_turso, "close": _close_turso, "run": _run_on_turso},
}


# --- what a scenario asks the database for -----------------------------------

NARROWING_NOTHING = {"each": {}, "any_of": [], "none_of": []}

# A caller's value that ends the statement, drops the table and comments out
# what follows, if anybody ever pastes one into text.
AN_AWKWARD_VALUE = "o'brien'); DROP TABLE people;--"


def _declaration(table: str, wanted: str, rows: list[dict] | None) -> dict:
    """A question about one table, written out the way `Declaration` declares it."""
    return {
        "model": table,
        "wanted": wanted,
        "columns": [],
        "condition": NARROWING_NOTHING,
        "order": {},
        "grouping": [],
        "having": NARROWING_NOTHING,
        "paging": None,
        "rows": rows,
        "across": None,
    }


def _a_table_of(table: str, kind: str) -> dict:
    """One schema operation about `table`, as `Operation` declares one."""
    return {
        "kind": kind,
        "table": table,
        "detail": {},
        "columns": [
            {
                "name": "id",
                "declared_type": "integer",
                "renamed_from": None,
                "references": None,
                "facets": {},
            },
            {
                "name": "note",
                "declared_type": "text",
                "renamed_from": None,
                "references": None,
                "facets": {},
            },
        ],
        "indexes": [],
        "constraints": [],
        "copying": [],
        "values": [],
        "destroys": False,
    }


def _compiled(db: dict, declaration: dict) -> dict:
    """The statement this question spells on this database's engine."""
    statement = compile_statement(declaration, db["engine"])
    assert "text" in statement, f"the question could not be compiled: {statement}"
    return statement


def _spelled(db: dict, kind: str, table: str) -> dict:
    """The one statement this schema change spells on this database's engine."""
    statements = spell_operation(_a_table_of(table, kind), db["engine"])
    assert isinstance(statements, list), f"the change could not be spelled: {statements}"
    return statements[0]


# --- running one piece of work on every engine -------------------------------


async def _against_every_engine(work: Callable[[dict, str], Awaitable[dict]]) -> dict[str, dict]:
    """Open each engine, run `work` on a table of its own, close everything.

    The table name is new every time, so nothing this file makes can meet
    anything another test left behind in the probe database.
    """
    answers: dict[str, dict] = {}
    for engine, driver in THE_THREE_ENGINES.items():
        table = "qrs_" + uuid.uuid4().hex[:12]
        with tempfile.TemporaryDirectory() as made:
            folder = pathlib.Path(made)
            connection = await driver["open"](folder)
            drainer = await driver["open"](folder)
            # CLOSED ON PURPOSE. A statement that reached the crew would fail here.
            await driver["close"](drainer)
            db = {
                "engine": engine,
                "connection": connection,
                "writers": [drainer],
                "room": {"arrivals": [], "waiting": []},
                "shutdown": "exit_immediately",
                "primary": None,
            }
            try:
                answers[engine] = await work(db, table)
            finally:
                await _drop_whatever_was_made(db, driver["run"], table)
                await driver["close"](connection)
    return answers


async def _drop_whatever_was_made(db: dict, run, table: str) -> None:
    """Take the table away again.

    PostgreSQL's probe database outlives this scenario, so a table left here is
    a table the next test meets. Two of the four scenarios never create one,
    which is why the drop is allowed to fail.
    """
    try:
        await run(db["connection"], _spelled(db, "drop_table", table)["text"])
    except Exception:  # noqa: BLE001 - the table may never have been created
        return


# --- the four pieces of work -------------------------------------------------


async def _a_select(db: dict, table: str) -> dict:
    await query_run_statement(db, _spelled(db, "create_table", table))
    await query_run_statement(
        db, _compiled(db, _declaration(table, "add", [{"id": 1, "note": "Alice"}]))
    )
    return {
        "answer": await query_run_statement(db, _compiled(db, _declaration(table, "all", None)))
    }


async def _a_create_table(db: dict, table: str) -> dict:
    """The migration's statement, and the same statement offered to a drainer."""
    made = await query_run_statement(db, _spelled(db, "create_table", table))
    read_back = await query_run_statement(db, _compiled(db, _declaration(table, "all", None)))
    return {
        "made": made,
        "read_back": read_back,
        "on_the_drainer": await _offered_to_the_drainer(db, table + "_by_a_drainer"),
    }


async def _offered_to_the_drainer(db: dict, table: str) -> object:
    """What a closed drainer's connection does with the same kind of statement.

    A NAME OF ITS OWN, because a second create of the first table would fail for
    the ordinary reason that it already exists, which says nothing about which
    connection ran it.
    """
    drainer = {**db, "connection": db["writers"][0]}
    try:
        return await query_run_statement(drainer, _spelled(drainer, "create_table", table))
    except Exception as refused:  # noqa: BLE001 - a closed connection may raise
        return refused


async def _a_refused_statement(db: dict, table: str) -> dict:
    """A select on a table nobody made, put twice: through the door, and raw."""
    asked = _compiled(db, _declaration(table + "_never_made", "all", None))
    return {
        "fault": await query_run_statement(db, asked),
        "the_engine_said": await _what_the_driver_says(db, asked["text"]),
    }


async def _what_the_driver_says(db: dict, text: str) -> str:
    """The engine's own words, taken from the engine rather than written here."""
    try:
        await THE_THREE_ENGINES[db["engine"]]["run"](db["connection"], text)
    except Exception as refused:  # noqa: BLE001 - this is the message we came for
        return str(refused)
    return ""


async def _a_stored_value(db: dict, table: str) -> dict:
    await query_run_statement(db, _spelled(db, "create_table", table))
    stored = await query_run_statement(
        db, _compiled(db, _declaration(table, "add", [{"id": 1, "note": AN_AWKWARD_VALUE}]))
    )
    return {
        "stored": stored,
        "read_back": await query_run_statement(db, _compiled(db, _declaration(table, "all", None))),
    }


# --- the one When ------------------------------------------------------------
#
# FOUR SENTENCES, ONE FUNCTION. Each opening differs only in which piece of work
# runs, so the difference is a table rather than four copies of one body.

WHAT_IS_RUN = {
    "a select is run": _a_select,
    "a migration runs a create table": _a_create_table,
    "a statement the engine refuses is run": _a_refused_statement,
    "a value carrying a quote and a DROP is stored and read back": _a_stored_value,
}


@when(
    parsers.re(
        r"(?P<sentence>a select is run"
        r"|a migration runs a create table"
        r"|a statement the engine refuses is run"
        r"|a value carrying a quote and a DROP is stored and read back)"
    )
)
def _the_statement_is_run(scenario: dict, require_postgresql: None, sentence: str) -> None:
    scenario["answers"] = asyncio.run(_against_every_engine(WHAT_IS_RUN[sentence]))


# --- what each scenario says has to be true ----------------------------------


@then("rows come back")
def _rows_come_back(scenario: dict) -> None:
    for engine, answer in scenario["answers"].items():
        rows = answer["answer"]

        assert isinstance(rows, list), (
            f"on {engine} a select came back as {type(rows).__name__} carrying "
            f"{rows!r}, and a read answers with rows"
        )
        assert rows == [{"id": 1, "note": "Alice"}], (
            f"on {engine} the one row stored came back as {rows!r}"
        )


@then("it ran on the connection the database holds, not on a drainer's")
def _it_ran_on_the_databases_own_connection(scenario: dict) -> None:
    for engine, answer in scenario["answers"].items():
        assert isinstance(answer["made"], int), (
            f"on {engine} the create table came back as {answer['made']!r}, so it did not run"
        )
        assert answer["read_back"] == [], (
            f"on {engine} the table the migration made reads as "
            f"{answer['read_back']!r}, so the create did not land on the "
            "connection the database holds"
        )
        # THE DRAINER'S CONNECTION IS CLOSED, so anything that reached it fails.
        # A create that came back with a count from there would mean this
        # function chose a writer, which is what `architecture.md` keeps DDL off.
        assert not isinstance(answer["on_the_drainer"], int), (
            f"on {engine} a create table ran to completion on a connection that "
            "was already closed, which cannot happen; the statement reached "
            "some other connection"
        )


@then("a fault comes back carrying the engine's own error")
def _a_fault_carries_the_engines_own_error(scenario: dict) -> None:
    for engine, answer in scenario["answers"].items():
        fault = answer["fault"]
        said = answer["the_engine_said"]

        assert said != "", (
            f"{engine} accepted a select on a table nobody made, so this "
            "scenario is asking about a statement the engine does not refuse"
        )
        assert isinstance(fault, dict) and fault.get("kind") == "refused", (
            f"on {engine} a refused statement came back as {fault!r}, and a "
            "failure travels by return as a fault of kind refused"
        )
        assert said in fault["what_went_wrong"], (
            f"on {engine} the fault says {fault['what_went_wrong']!r} and the "
            f"engine said {said!r}; a reader who cannot see the engine's own "
            "words has to guess what the engine objected to"
        )
        assert fault["what_to_do_instead"] != "", (
            f"on {engine} the fault names what broke and nothing to do about it"
        )


@then("the value comes back character for character, and the table is still there")
def _the_value_comes_back_as_written(scenario: dict) -> None:
    for engine, answer in scenario["answers"].items():
        rows = answer["read_back"]

        assert rows == [{"id": 1, "note": AN_AWKWARD_VALUE}], (
            f"on {engine} the row reads as {rows!r}. The value was stored as "
            f"{AN_AWKWARD_VALUE!r}, and anything else means it reached the text "
            "of the statement instead of travelling beside it"
        )
        assert isinstance(answer["stored"], int), (
            f"on {engine} storing the value came back as {answer['stored']!r}"
        )
