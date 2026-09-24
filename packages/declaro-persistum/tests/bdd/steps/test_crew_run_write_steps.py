"""Bind `tests/bdd/features/crew/crew_run_write.feature`, and nothing else.

WHAT THE FEATURE ASKS. Three results about one deposited write: it runs on the
connection it was handed and no other, it says how many rows it touched, and a
failure that lost a race is told apart from one the engine refused.

EVERY SCENARIO RUNS ON ALL THREE ENGINES, INSIDE THE SCENARIO. The feature says
nothing about engines, because the result is the same on each;
`boundary_out fn crew_run_write` is one door with a row per driver behind it,
and a row nobody runs is a row nobody has seen work. So each scenario opens
SQLite, PostgreSQL and Turso, runs the same write on each, and asserts the same
result for each.

NOTHING HERE IS A STAND-IN. A double that got a driver contract wrong has
manufactured a defect in this package three times. SQLite is `aiosqlite`,
PostgreSQL is `asyncpg` on the probe database this pytest process made, and
Turso is `pyturso` on a file of its own with `journal_mode = 'mvcc'` fetched,
because an unfetched pragma is a silent no-op and `BEGIN CONCURRENT` is refused
without it.

THE WRITER IS A BARE CONNECTION, NOT A POOL. That is what
`boundary_out fn session_open_writer` opens for every engine, PostgreSQL
included: a drainer holds one connection for its life and a pool has no
`transaction` of its own.

NO SQL IS WRITTEN HERE. `fn spell_operation` writes the create table and
`fn compile_statement` writes every insert and update, each for the engine it is
about to run on, which is also what puts the driver's own placeholders in the
text and the caller's values beside it.

HOW A CONFLICT IS MADE, ENGINE BY ENGINE, AND WHY IT IS NOT ONE RECIPE. SQLite
takes a write lock, so a second connection holding `BEGIN IMMEDIATE` makes the
first one busy. Turso runs MVCC and detects at the row, so two concurrent
transactions touching one row make a write-write conflict. PostgreSQL only
serializes under an isolation level this package does not set, so its conflict
is made with an advisory lock held by a second connection. Each is that engine's
own way of losing a race, which is the thing being told apart from a refusal.
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import tempfile
from collections.abc import Awaitable, Callable

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.compile_statement import compile_statement
from declaro_persistum.crew_run_write import crew_run_write
from declaro_persistum.how_to_write import how_to_write
from declaro_persistum.spell_operation import spell_operation
from declaro_persistum.worth_retrying import worth_retrying
from tests.conftest import (
    NARROWS_BY_NOTHING,
    POSTGRESQL_PROBE_URL,
    a_column,
    a_constraint,
    an_operation,
)

scenarios("../features/crew/crew_run_write.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# The rows a scenario writes, and the two it changes when it asks for a count.
TWO_ROWS = [{"id": 1, "note": "one"}, {"id": 2, "note": "two"}]
WHAT_THE_CHANGE_WRITES = "changed"

# A column no model declares, so a refusal can be told from every other refusal.
A_COLUMN_NO_MODEL_DECLARES = "nickname"


# --- the three engines, opened for real --------------------------------------
#
# ONE ROW PER ENGINE, FOUR ENTRIES EACH: how a writer is opened, how it is
# closed, how a second connection is opened to race it, and how that second one
# takes whatever this engine loses a race on.


async def _open_sqlite(folder: pathlib.Path):
    import aiosqlite

    return await aiosqlite.connect(folder / "sqlite.db", isolation_level=None)


async def _close_sqlite(connection) -> None:
    await connection.close()


async def _sqlite_loses_a_race(folder: pathlib.Path, run_the_write):
    """A second connection holding the write lock, which is what busies SQLite.

    THE LOCK IS TAKEN FIRST AND HELD THROUGHOUT. SQLite has one writer, so the
    write under test cannot begin while this transaction is open, and the
    driver's short timeout turns the wait into SQLITE_BUSY rather than a hang.
    """
    import aiosqlite

    rival = await aiosqlite.connect(folder / "sqlite.db", timeout=0.05, isolation_level=None)
    await rival.execute("BEGIN IMMEDIATE")
    await rival.execute("UPDATE parts SET note = 'rival' WHERE id = 1")
    try:
        return await run_the_write()
    finally:
        await rival.rollback()
        await rival.close()


async def _open_turso(folder: pathlib.Path):
    """`journal_mode = 'mvcc'` fetched, because `BEGIN CONCURRENT` needs it.

    Measured 2026-09-24: without the fetch the pragma answers `mvcc` and the
    transaction is refused, which is the trap `fn session_open_connection`
    documents beside WAL.
    """
    import turso.aio

    connection = await turso.aio.connect(str(folder / "turso.db"))
    cursor = await connection.execute("PRAGMA journal_mode = 'mvcc'")
    await cursor.fetchall()
    return connection


async def _close_turso(connection) -> None:
    await connection.close()


async def _turso_loses_a_race(folder: pathlib.Path, run_the_write):
    """Two concurrent transactions on one row, and this one commits first.

    MVCC DETECTS AT THE ROW AND AT COMMIT. So the write under test has to be in
    flight, with its own transaction open and its own update done, before this
    one commits. It is started as a task and given a turn to get that far.
    """
    rival = await _open_turso(folder)
    cursor = rival.cursor()
    await cursor.execute("BEGIN CONCURRENT")
    await cursor.execute("UPDATE parts SET note = 'rival' WHERE id = 1")
    writing = asyncio.ensure_future(run_the_write())
    await asyncio.sleep(0.05)
    try:
        await rival.commit()
    finally:
        answer = await writing
        await rival.close()
    return answer


async def _open_postgresql(_folder: pathlib.Path):
    """A bare connection, which is what a drainer holds."""
    import asyncpg

    return await asyncpg.connect(POSTGRESQL_PROBE_URL)


async def _close_postgresql(connection) -> None:
    await connection.close()


async def _postgresql_loses_a_race(_folder: pathlib.Path, run_the_write):
    """A deadlock, because a plain row lock here is a wait rather than a failure.

    POSTGRESQL DOES NOT SERIALIZE AT THIS ISOLATION LEVEL, so a contended write
    waits for the lock and then succeeds; it never comes back as a failure and
    there is nothing to classify. A deadlock is the contention this engine does
    report, as `DeadlockDetectedError`, and it needs no setting changed.

    THE TWO TAKE THE ROWS IN OPPOSITE ORDERS, which is what a deadlock is. This
    one holds row 2. The write under test reaches both rows, takes row 1 and
    waits for row 2. Then this one asks for row 1, and the engine kills whichever
    of the two it picks.
    """
    import asyncpg

    rival = await asyncpg.connect(POSTGRESQL_PROBE_URL)
    holding = rival.transaction()
    await holding.start()
    await rival.execute("UPDATE parts SET note = 'rival' WHERE id = 2")
    writing = asyncio.ensure_future(run_the_write())
    await asyncio.sleep(0.2)
    # THE ENGINE PICKS ONE OF THE TWO TO KILL, and it may pick this one. That is
    # the other half of the same deadlock and is fine: the write under test is
    # the one being read, and whichever of them the engine chose, it reported a
    # deadlock to somebody.
    with contextlib.suppress(asyncpg.PostgresError):
        await rival.execute("UPDATE parts SET note = 'rival' WHERE id = 1")
    answer = await writing
    with contextlib.suppress(asyncpg.PostgresError):
        await holding.rollback()
    await rival.close()
    return answer


THE_THREE_ENGINES: dict[str, dict[str, Callable]] = {
    "sqlite": {
        "open": _open_sqlite,
        "close": _close_sqlite,
        "race": _sqlite_loses_a_race,
    },
    "turso": {
        "open": _open_turso,
        "close": _close_turso,
        "race": _turso_loses_a_race,
    },
    "postgresql": {
        "open": _open_postgresql,
        "close": _close_postgresql,
        "race": _postgresql_loses_a_race,
    },
}


# --- what a scenario declares ------------------------------------------------


def _declaration(table: str, wanted: str, rows: list[dict] | None, condition: dict) -> dict:
    """One thing a caller wants, written out the way `Declaration` declares it."""
    return {
        "model": table,
        "wanted": wanted,
        "columns": [],
        "condition": condition,
        "order": {},
        "grouping": [],
        "having": NARROWS_BY_NOTHING,
        "paging": None,
        "rows": rows,
        "shaping": "dictionaries",
        "across": None,
    }


def _every_row() -> dict:
    return NARROWS_BY_NOTHING


def _a_table_with_a_key(table: str) -> dict:
    """A create table whose `id` is the key, as `Operation` declares one."""
    return an_operation(
        kind="create_table",
        table=table,
        column=None,
        columns=[
            a_column(name="id", declared_type="integer", renamed_from=None, references=None),
            a_column(name="note", declared_type="text", renamed_from=None, references=None),
        ],
        indexes=[],
        constraints=[
            a_constraint(
                name=f"{table}_pk",
                kind="primary_key",
                columns=["id"],
                rule=NARROWS_BY_NOTHING,
                value=None,
            )
        ],
        copying=[],
        values=[],
        renamed_to=None,
        view=None,
        destroys=False,
    )


def _compiled(engine: str, declaration: dict) -> dict:
    statement = compile_statement(declaration, engine)
    assert "text" in statement, f"the declaration could not be compiled: {statement}"
    return statement


def _spelled(engine: str, operation: dict) -> dict:
    statements = spell_operation(operation, engine)
    assert isinstance(statements, list), f"the change could not be spelled: {statements}"
    return statements[0]


# --- opening every engine and running one write on each ----------------------


async def _against_every_engine(
    work: Callable[[str, object, str, pathlib.Path], Awaitable[dict]],
) -> dict[str, dict]:
    """Open each engine on a table of its own, run `work`, close everything.

    THE TABLE IS CALLED `parts` ON EVERY ENGINE and lives in a schema of its own
    for PostgreSQL, so the rival connections above can name it without being
    handed a name. SQLite and Turso get a directory of their own each time.
    """
    answers: dict[str, dict] = {}
    for engine, driver in THE_THREE_ENGINES.items():
        with tempfile.TemporaryDirectory() as made:
            folder = pathlib.Path(made)
            writer = await driver["open"](folder)
            try:
                await _make_the_table(writer, engine)
                answers[engine] = await work(engine, writer, "parts", folder)
            finally:
                await _take_the_table_away(writer, engine)
                await driver["close"](writer)
    return answers


async def _make_the_table(writer, engine: str) -> None:
    """The table, and the two rows every scenario starts from."""
    await _run_plainly(writer, engine, _spelled(engine, _a_table_with_a_key("parts")))
    for row in TWO_ROWS:
        await _run_plainly(
            writer, engine, _compiled(engine, _declaration("parts", "add", [row], _every_row()))
        )


async def _take_the_table_away(writer, engine: str) -> None:
    """PostgreSQL's probe database outlives this scenario."""
    dropping = {**_a_table_with_a_key("parts"), "kind": "drop_table"}
    await _run_plainly(writer, engine, _spelled(engine, dropping))


# HOW A SCENARIO IS SET UP AND READ BACK, ONE ROW PER DRIVER. Not through
# `fn query_run_statement`: that door acquires a connection from a pool, which
# is what `fn session_open_connection` opens for PostgreSQL reads, and a drainer
# holds a bare connection. Setting a scenario up through a door that cannot take
# the handle under test would be testing a type production never sees.
async def _sqlite_run(writer, text: str, values: list):
    cursor = await writer.execute(text, values)
    return await cursor.fetchall(), [c[0] for c in (cursor.description or [])]


async def _turso_run(writer, text: str, values: list):
    cursor = writer.cursor()
    await cursor.execute(text, values)
    rows = await cursor.fetchall() if cursor.description else []
    await writer.commit()
    return rows, [c[0] for c in (cursor.description or [])]


async def _postgresql_run(writer, text: str, values: list):
    records = await writer.fetch(text, *values)
    return [tuple(record) for record in records], list(records[0].keys()) if records else []


_RUN_DIRECTLY: dict[str, Callable] = {
    "sqlite": _sqlite_run,
    "turso": _turso_run,
    "postgresql": _postgresql_run,
}


async def _run_plainly(writer, engine: str, statement: dict) -> list[dict]:
    """Run one statement on the bare writer, and hand back whatever it gave."""
    rows, names = await _RUN_DIRECTLY[engine](
        writer, statement["text"], list(statement["parameters"].values())
    )
    return [dict(zip(names, row, strict=True)) for row in rows]


# --- what each scenario does -------------------------------------------------


@pytest.fixture
def scenario() -> dict:
    return {}


async def _one_write(engine: str, writer, table: str, _folder: pathlib.Path) -> dict:
    """One ordinary write, run through the door under test."""
    statement = _compiled(
        engine, _declaration(table, "add", [{"id": 9, "note": "nine"}], _every_row())
    )
    answer = await crew_run_write(writer, engine, statement, how_to_write(engine))
    rows = await _run_plainly(
        writer, engine, _compiled(engine, _declaration(table, "all", None, _every_row()))
    )
    return {"answer": answer, "rows": rows, "writer": writer, "statement": statement}


async def _a_change_of_two_rows(engine: str, writer, table: str, _folder: pathlib.Path) -> dict:
    """One statement that reaches both rows the table holds."""
    statement = _compiled(
        engine,
        _declaration(
            table,
            "change",
            [{"note": WHAT_THE_CHANGE_WRITES}],
            {
                "each": {"id": {"operator": "lte", "value": len(TWO_ROWS), "ignore_case": False}},
                "any_of": [],
                "none_of": [],
            },
        ),
    )
    return {"answer": await crew_run_write(writer, engine, statement, how_to_write(engine))}


async def _a_write_that_loses_a_race(engine: str, writer, table: str, folder: pathlib.Path) -> dict:
    """A second connection racing this write for the same rows.

    THE WRITE REACHES BOTH ROWS, because PostgreSQL's contention is a deadlock
    and a deadlock needs two rows taken in opposite orders. SQLite and Turso lose
    the race on the first of them.
    """
    statement = _compiled(
        engine,
        _declaration(
            table,
            "change",
            [{"note": "mine"}],
            {
                "each": {"id": {"operator": "lte", "value": len(TWO_ROWS), "ignore_case": False}},
                "any_of": [],
                "none_of": [],
            },
        ),
    )

    async def _run_the_write():
        return await crew_run_write(writer, engine, statement, how_to_write(engine))

    answer = await asyncio.wait_for(THE_THREE_ENGINES[engine]["race"](folder, _run_the_write), 20.0)
    return {"answer": answer}


async def _a_write_the_engine_refuses(
    engine: str, writer, table: str, _folder: pathlib.Path
) -> dict:
    """A write naming a column no model declares, which will never work."""
    statement = _compiled(
        engine,
        _declaration(
            table, "add", [{"id": 9, A_COLUMN_NO_MODEL_DECLARES: "no such column"}], _every_row()
        ),
    )
    return {"answer": await crew_run_write(writer, engine, statement, how_to_write(engine))}


WHAT_EACH_WHEN_DOES: dict[str, Callable] = {
    "a drainer runs a write": _one_write,
    "a write changes two rows": _a_change_of_two_rows,
    "a write conflicts at commit": _a_write_that_loses_a_race,
    "a write names a column no model declares": _a_write_the_engine_refuses,
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@when(parsers.re(_one_of(tuple(sorted(WHAT_EACH_WHEN_DOES, key=len, reverse=True)))))
def _what_was_asked_for(scenario: dict, sentence: str) -> None:
    """Four sentences, one function over a table, run on all three engines."""
    scenario["outcomes"] = asyncio.run(_against_every_engine(WHAT_EACH_WHEN_DOES[sentence]))


# --- what must be true afterwards --------------------------------------------


def _it_ran_on_that_connection(engine: str, outcome: dict) -> None:
    """The rows are in the database the writer was opened on, and nowhere else."""
    answer, rows = outcome["answer"], outcome["rows"]

    assert isinstance(answer, int), (
        f"on {engine} the write answered {answer!r}, and a write that landed "
        f"answers with how many rows it touched"
    )
    written = [row for row in rows if row["id"] == 9]
    assert len(written) == 1, (
        f"on {engine} the write answered {answer} and the table the writer was "
        f"opened on holds {rows!r}, so the row went somewhere else"
    )


def _the_answer_is_two(engine: str, outcome: dict) -> None:
    assert outcome["answer"] == len(TWO_ROWS), (
        f"on {engine} a change reaching {len(TWO_ROWS)} rows answered {outcome['answer']!r}"
    )


def _it_is_worth_trying_again(engine: str, outcome: dict) -> None:
    """The fault's kind says retry, which is the only thing that reads it."""
    fault = outcome["answer"]

    assert isinstance(fault, dict) and set(fault) == THE_THREE_FACTS_IN_A_FAULT, (
        f"on {engine} a write that lost a race came back as {fault!r}, and a "
        f"failure comes back as a Fault in the return"
    )
    assert worth_retrying(fault), (
        f"on {engine} a write that lost a race came back as kind "
        f"{fault['kind']!r}, which `fn worth_retrying` reads as not worth "
        f"another attempt, so the caller loses a write that would have landed"
    )


def _it_is_not_worth_trying_again(engine: str, outcome: dict) -> None:
    """The control. Without it every conflict scenario passes on a function that
    calls every failure a conflict, which spins a budget on a write that will
    never run."""
    fault = outcome["answer"]

    assert isinstance(fault, dict) and set(fault) == THE_THREE_FACTS_IN_A_FAULT, (
        f"on {engine} a write naming a column nobody declared came back as {fault!r}"
    )
    assert not worth_retrying(fault), (
        f"on {engine} a write the engine refused came back as kind "
        f"{fault['kind']!r}, which `fn worth_retrying` reads as worth another "
        f"attempt, so a write that can never run is run until the budget goes"
    )


WHAT_EACH_THEN_CHECKS: dict[str, Callable[[str, dict], None]] = {
    "it ran on that drainer's connection": _it_ran_on_that_connection,
    "the answer is two": _the_answer_is_two,
    "the fault says it is worth trying again": _it_is_worth_trying_again,
    "the fault says it is not worth trying again": _it_is_not_worth_trying_again,
}


@then(parsers.re(_one_of(tuple(sorted(WHAT_EACH_THEN_CHECKS, key=len, reverse=True)))))
def _what_must_be_true(scenario: dict, sentence: str) -> None:
    """Four sentences, one function over a table, asked of all three engines.

    THE COUNT IS CHECKED BEFORE THE LOOP, because a loop over nothing runs no
    assertion and publishes a pass. An engine that failed to open, or a `When`
    that answered for two of the three, would otherwise come back green with the
    row nobody ran still unrun.
    """
    outcomes = scenario["outcomes"]
    assert set(outcomes) == set(THE_THREE_ENGINES), (
        f"{sentence!r} was asked of {sorted(outcomes)}, and this file exists to "
        f"run every row behind the door: {sorted(THE_THREE_ENGINES)}"
    )
    for engine, outcome in outcomes.items():
        WHAT_EACH_THEN_CHECKS[sentence](engine, outcome)
