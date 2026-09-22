"""Bind `tests/bdd/features/query/query_answer.feature`, and nothing else.

WHAT THE FEATURE ASKS. Five scenarios about one orchestrator. `orchestrator fn
query_answer` is handed an `Answering` carrying the database, the model, the
grammar and what the caller said, builds the declaration, refuses a column no
model declares, compiles the statement for this engine, and then either runs it
now or deposits it and hands back a ticket.

THE FORK IS THE WHOLE OF IT. A question runs now and answers now. A write of one
row goes into the waiting room and is answered by a ticket at once, and the row
reaches the engine later. Several rows handed to one call are neither: they land
together or not at all, so they hold a connection instead of taking a ticket.
Three of the five scenarios are one of those three paths, and each is read off
what is in the database and what came back, never off which function ran.

THE FIFTH WAS ADDED WHILE THIS WAS BEING BUILT. The four spellings a caller may
ask in are a lookup with no fallback, so a word with no builder has to come back
refused. Nothing in the feature asked for that, and the refusal would have
shipped unrun.

EVERY DATABASE HERE IS A REAL ONE, OPENED THROUGH THE DOOR. A stand-in that got
a driver contract wrong has manufactured three defects in this package this
week. `boundary_in fn session_open` opens each of the three: SQLite through
aiosqlite, PostgreSQL through asyncpg against a database this scenario makes for
itself, and Turso through pyturso on a `libsql` target, which is the only thing
in a target that names that engine. Opening through the door is also what makes
the table exist, because the door migrates the models before it hands a handle
back.

EVERY SCENARIO RUNS ON ALL THREE, INSIDE THE SCENARIO. The feature names no
engine, because the answer is the same on each, and a row nobody runs is a row
nobody has seen work. Every `Then` below says how many engines it asked, because
a loop over an empty set of outcomes passes while asking nothing.

NO SQL IS WRITTEN HERE. `fn compile_statement` writes every statement, for the
engine it is about to run on, and this file reads rows back through the same
door a caller uses. The two catalogue questions in `_what_the_table_holds` are
the exception and are the test's own, because asking an engine what it holds is
not something the package does for a caller.

WHAT THIS CHAIN CANNOT BE ASKED TODAY, stated here rather than hidden in a skip.
`fn join_between` is on this orchestrator's card and nothing below reaches it.
`type Answering` carries one `model: Model`, and `fn join_between` is declared
over two, so a question across models has no far model to be worked out from.
No scenario in this feature asks for one, and inventing a shape for it here
would be inventing the shape in `type Declaration` as well.
"""

from __future__ import annotations

import asyncio
import gc
import hashlib
import os
import pathlib
import re
import signal
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from pydantic import BaseModel
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.lifecycle_install_handler import (
    WHAT_THE_PLATFORM_SENDS,
    WHAT_WAS_THERE,
)
from declaro_persistum.lifecycle_register import REGISTERED
from declaro_persistum.query_answer import BY_GRAMMAR, query_answer
from declaro_persistum.session_open import session_open
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/query/query_answer.feature")


# =============================================================================
# what the feature's people are
# =============================================================================

THE_TABLE_THE_FEATURE_NAMES = "people"

# THE CLASS'S OWN NAME, AND IT IS NOT THE TABLE'S. A host writes `class Person`
# and `__tablename__ = "people"`, so a caller has `Person` to ask with and the
# engine has `people` to answer to. Until 2026-09-22 this file called the class
# `people` as well, which made the two words one word, and no scenario here
# could tell whether anything traded one for the other. Nothing did: every
# question a caller wrote by class name reached the engine as `FROM Person`.
THE_NAME_THE_CLASS_HAS = "Person"

# The rows a scenario puts in the table before it asks anything, and the three
# it declares together. `id` is the key, so the group scenario has a row the
# engine will refuse when it needs one.
ALREADY_THERE = [
    {"id": 1, "name": "alice"},
    {"id": 2, "name": "bob"},
    {"id": 3, "name": "carol"},
]
ONE_NEW_PERSON = {"id": 4, "name": "dave"}
THREE_TOGETHER = [
    {"id": 5, "name": "erin"},
    {"id": 6, "name": "frank"},
    {"id": 7, "name": "grace"},
]
THE_NAME_THE_FEATURE_ASKS_FOR = "bob"

# What a group that cannot land declares: the middle row claims a key that is
# already taken, and the two beside it are rows any engine would accept.
A_GROUP_ONE_OF_WHICH_IS_REFUSED = [
    {"id": 8, "name": "heidi"},
    {"id": 1, "name": "ivan"},
    {"id": 9, "name": "judy"},
]


NARROWING_NOTHING = {"each": {}, "any_of": [], "none_of": []}

# `id` is the key, declared as a constraint on the model the way a host declares
# one, so the engine refuses a second row claiming a key it already holds.
THE_KEY: dict[str, Any] = {
    "name": "people_id",
    "kind": "primary_key",
    "columns": ["id"],
    "rule": NARROWING_NOTHING,
}


def _a_person_model() -> type:
    """The model the feature names, declared the way a host declares one.

    DECLARED INSIDE A CALL AND NEVER AT MODULE LEVEL. `reader fn
    schema_read_declared` reads every Pydantic class alive in the process, so a
    class held by this module would be part of every other test's migration for
    the rest of the run.
    """

    class Person(BaseModel):
        __tablename__ = THE_TABLE_THE_FEATURE_NAMES
        # THE KEY IS WHAT LETS A GROUP BE REFUSED. Without it no write in this
        # file is one an engine turns down for a reason a caller could have
        # caused, and "they run as one transaction" would be asserted against a
        # group nothing could stop.
        __constraints__ = [THE_KEY]
        id: int
        name: str

    return Person


# =============================================================================
# what a caller hands the orchestrator
# =============================================================================
#
# THE MODEL AS THE PACKAGE HOLDS IT. `type Model` is the shape `reader fn
# schema_read_declared` produces, and the orchestrator is handed one rather than
# a Pydantic class.

THE_MODEL: dict[str, Any] = {
    "name": THE_NAME_THE_CLASS_HAS,
    "table": THE_TABLE_THE_FEATURE_NAMES,
    "columns": [
        {
            "name": "id",
            "declared_type": "integer",
            "renamed_from": None,
            "references": None,
            "facets": {},
        },
        {
            "name": "name",
            "declared_type": "text",
            "renamed_from": None,
            "references": None,
            "facets": {},
        },
    ],
    "dropping": [],
    "indexes": [],
    "constraints": [THE_KEY],
}

# THE GRAMMAR THE FEATURE SAYS NOTHING ABOUT. Four spellings reach one
# declaration, and which one a caller used is `query_ask.feature`'s question.
# This file says the library's own, once, so nothing here rests on a spelling.
THE_GRAMMAR = "pseudo_sql"


# WHAT NO BUILDER ANSWERS TO. One scenario asks in it, and the whole of what it
# has to be is a word that is not one of the four.
A_SPELLING_NOTHING_CAN_BUILD = "hibernate"

# THE FOUR A REFUSAL HAS TO LIST, read off the table the orchestrator dispatches
# on rather than typed again, so a fifth spelling could not be added without
# this scenario asking for it.
THE_FOUR_SPELLINGS = tuple(sorted(BY_GRAMMAR))


def _an_answering(db: dict[str, Any], asked: dict[str, Any], grammar: str) -> dict[str, Any]:
    """What a caller fills, and nothing else.

    `type Answering` names eight fields and a caller fills four. The other four
    are what each step of the chain produced, so they are absent here rather
    than filled with a value nobody decided.
    """
    return {"db": db, "model": THE_MODEL, "grammar": grammar, "asked": asked}


def _asking_for(**said: Any) -> dict[str, Any]:
    """One question in the library's own words."""
    return {"model": THE_TABLE_THE_FEATURE_NAMES, **said}


# =============================================================================
# the three engines, each opened through the door
# =============================================================================


async def _a_postgresql_database_of_our_own(folder: pathlib.Path, node: str) -> str:
    """One PostgreSQL database named for this test, dropped afterwards.

    Opening migrates the models, and a migration plans a drop for every table
    the models do not declare, so a shared database is one this file would
    empty. The boundary has to be the database.
    """
    named = "qa_" + hashlib.sha256(node.encode()).hexdigest()[:16]
    server, _, _ = POSTGRESQL_PROBE_URL.rpartition("/")
    await _administer([f'DROP DATABASE IF EXISTS "{named}"', f'CREATE DATABASE "{named}"'])
    return f"{server}/{named}"


async def _administer(statements: list[str]) -> None:
    """Make and unmake a database, on the probe server this process owns."""
    import asyncpg

    connection = await asyncpg.connect(POSTGRESQL_PROBE_URL)
    try:
        for one in statements:
            await connection.execute(one)
    finally:
        await connection.close()


async def _a_sqlite_file_in(folder: pathlib.Path, _node: str) -> str:
    return str(folder / "asked.db")


async def _a_turso_file_in(_folder: pathlib.Path, _node: str) -> str:
    """A `libsql` target is the only thing that names Turso, and pyturso reads a
    relative path, so the folder this scenario owns is the working directory."""
    return "libsql://asked.db"


WHERE_EACH_ENGINES_DATABASE_GOES: dict[str, Callable[[pathlib.Path, str], Awaitable[str]]] = {
    "sqlite": _a_sqlite_file_in,
    "postgresql": _a_postgresql_database_of_our_own,
    "turso": _a_turso_file_in,
}

# WHAT THE DEPLOYMENT STATES, because `boundary_in fn session_open` reads both
# and neither has a default. One connection and one drainer is the smallest
# deployment there is, and this file never drains.
WHAT_THE_DEPLOYMENT_STATES = {"DECLARO_CONNECTIONS": "1", "DECLARO_CREW_SIZE": "1"}

# The catalogue question this file asks each engine to read a table back with.
# It is the test's own, and the only statement written here.
WHAT_THE_TABLE_HOLDS: dict[str, str] = {
    "sqlite": f"SELECT id, name FROM {THE_TABLE_THE_FEATURE_NAMES} ORDER BY id",
    "postgresql": f"SELECT id, name FROM {THE_TABLE_THE_FEATURE_NAMES} ORDER BY id",
    "turso": f"SELECT id, name FROM {THE_TABLE_THE_FEATURE_NAMES} ORDER BY id",
}


async def _what_the_table_holds(db: dict[str, Any]) -> list[dict[str, Any]]:
    """Every row in the table now, read straight off the driver.

    Read here rather than through another question, because a question is what
    the scenario is about and an answer that came back wrong would agree with
    itself.
    """
    from declaro_persistum.query_run_statement import query_run_statement

    rows = await query_run_statement(
        db, {"text": WHAT_THE_TABLE_HOLDS[db["engine"]], "parameters": {}}
    )
    assert isinstance(rows, list), f"the table could not be read back: {rows}"
    return [{"id": row["id"], "name": row["name"]} for row in rows]


async def _opened(target: str) -> dict[str, Any]:
    """One database, with the feature's table already migrated into it."""
    db = await session_open(target, "exit_immediately")
    assert not _is_a_fault(db), f"opening {target} had to work and it said {db}"
    return db


async def _filled(db: dict[str, Any]) -> None:
    """The three people every scenario starts from."""
    from declaro_persistum.compile_statement import compile_statement
    from declaro_persistum.query_run_statement import query_run_statement

    statement = compile_statement(_a_declaration("add", rows=ALREADY_THERE), db["engine"])
    assert "text" in statement, f"the three people could not be compiled: {statement}"
    answered = await query_run_statement(db, statement)
    assert not _is_a_fault(answered), f"the three people did not land: {answered}"


def _a_declaration(wanted: str, rows: list[dict[str, Any]] | None) -> dict[str, Any]:
    """One write, written out the way `type Declaration` declares one.

    Only the setup uses this. What a scenario is about is built by the
    orchestrator from what the caller said.
    """
    return {
        "model": THE_TABLE_THE_FEATURE_NAMES,
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


async def _closed(db: dict[str, Any]) -> None:
    """Everything this database opened, closed on the loop that opened it.

    aiosqlite runs one non-daemon thread per connection, so a leaked one holds
    the interpreter open after every test has finished.
    """
    for handle in [db["connection"], *db["writers"]]:
        await handle.close()


async def _goes_with_the_folder(_target: str) -> list[str]:
    """A file in the folder pytest made needs nothing taken away.

    IT ANSWERS WITH A LIST RATHER THAN RETURNING NOTHING. Both halves of this
    pair hand back what they took away, so the caller reads an answer instead of
    trusting that a side effect happened, and an empty body could not be told
    from one nobody has written.
    """
    return []


async def _taken_off_the_server(target: str) -> list[str]:
    """A PostgreSQL database this file made, dropped again, and named."""
    named = target.rpartition("/")[2]
    await _administer([f'DROP DATABASE IF EXISTS "{named}" WITH (FORCE)'])
    return [named]


# What each engine's database costs to leave behind. A file goes with the folder
# pytest made; a database on a server does not, and the probe server outlives
# every scenario in this run.
WHAT_BECOMES_OF_EACH_DATABASE: dict[str, Callable[[str], Awaitable[list[str]]]] = {
    "sqlite": _goes_with_the_folder,
    "postgresql": _taken_off_the_server,
    "turso": _goes_with_the_folder,
}

# How many of the three leave something behind that this file has to drop.
DATABASES_ON_A_SERVER = 1


async def _against_every_engine(
    work: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    folder: pathlib.Path,
    node: str,
) -> dict[str, dict[str, Any]]:
    """Open each engine, run `work` against the feature's table, close it all."""
    answers: dict[str, dict[str, Any]] = {}
    taken_away: list[str] = []
    for engine in WHERE_EACH_ENGINES_DATABASE_GOES:
        mine = folder / (engine + "_" + uuid.uuid4().hex[:8])
        mine.mkdir()
        was = os.getcwd()
        os.chdir(mine)
        target = await WHERE_EACH_ENGINES_DATABASE_GOES[engine](mine, node)
        try:
            db = await _opened(target)
            try:
                await _filled(db)
                answers[engine] = await work(db)
            finally:
                await _closed(db)
        finally:
            os.chdir(was)
            taken_away.extend(await WHAT_BECOMES_OF_EACH_DATABASE[engine](target))
    assert len(taken_away) == DATABASES_ON_A_SERVER, (
        f"this scenario made {len(taken_away)} databases on a server and dropped "
        f"{taken_away}. The probe server outlives it, so one left behind is one "
        "the next test meets"
    )
    return answers


# =============================================================================
# what the process is owed back
# =============================================================================


@pytest.fixture(autouse=True)
def the_process_is_left_as_it_was(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Put back the environment and the three pieces of process state an open
    changes.

    `boundary_in fn session_open` reads two variables and restores neither, and
    opening a database is startup, so it takes the process's signals and writes
    itself onto the shutdown list. All three are right for a library and wrong
    to leave behind in a test run.
    """
    for name, number in WHAT_THE_DEPLOYMENT_STATES.items():
        monkeypatch.setenv(name, number)
    held = {number: signal.getsignal(number) for number in WHAT_THE_PLATFORM_SENDS}
    was_recorded = dict(WHAT_WAS_THERE)
    was_registered = list(REGISTERED)
    gc.collect()
    try:
        yield
    finally:
        for number, handler in held.items():
            signal.signal(number, handler)
        WHAT_WAS_THERE.clear()
        WHAT_WAS_THERE.update(was_recorded)
        REGISTERED[:] = was_registered
        gc.collect()


@pytest.fixture(autouse=True)
def the_model_the_feature_names(scenario: dict[str, object]) -> Any:
    """Declared for the length of this scenario and let go afterwards."""
    scenario["model_class"] = _a_person_model()
    yield
    del scenario["model_class"]
    gc.collect()


# =============================================================================
# telling a failure from an answer
# =============================================================================

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _is_a_fault(answered: object) -> bool:
    return isinstance(answered, dict) and frozenset(answered) == THE_FIELDS_OF_A_FAULT


def _room_of(db: dict[str, Any]) -> dict[str, Any]:
    """What is waiting in the room, copied, so a later deposit cannot change it."""
    return {
        "arrivals": list(db["room"]["arrivals"]),
        "writes": dict(db["room"]["writes"]),
        "receipts": dict(db["room"]["receipts"]),
    }


# =============================================================================
# the four questions a scenario asks
# =============================================================================


async def _every_person(db: dict[str, Any]) -> dict[str, Any]:
    """A read. It runs now and answers now."""
    answered = await query_answer(_an_answering(db, _asking_for(wanted="all"), THE_GRAMMAR))
    return {
        "answered": answered,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _every_person_by_the_class_name(db: dict[str, Any]) -> dict[str, Any]:
    """The same read, written the way a host's caller writes one.

    The question says `Person`, which is the class's own name and the only name
    a caller has. The rows live in `people`. If nothing trades one word for the
    other, the engine is sent `SELECT ... FROM Person` and answers that there is
    no such table.
    """
    answered = await query_answer(
        _an_answering(db, {"model": THE_NAME_THE_CLASS_HAS, "wanted": "all"}, THE_GRAMMAR)
    )
    return {
        "answered": answered,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _one_new_person(db: dict[str, Any]) -> dict[str, Any]:
    """A write of one row. It is deposited and answered by a ticket."""
    answered = await query_answer(
        _an_answering(db, _asking_for(wanted="add", rows=[ONE_NEW_PERSON]), THE_GRAMMAR)
    )
    return {
        "answered": answered,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _three_people_together(db: dict[str, Any]) -> dict[str, Any]:
    """Three rows in one call, and then a group the engine will not take.

    THE SECOND GROUP IS WHAT MAKES "one transaction" AN ASSERTION. A group that
    lands looks the same as three deposits that all worked. A group whose middle
    row claims a key that is taken leaves nothing behind when it is one
    transaction, and leaves the row before it when it is not.
    """
    answered = await query_answer(
        _an_answering(db, _asking_for(wanted="add", rows=THREE_TOGETHER), THE_GRAMMAR)
    )
    landed = {
        "answered": answered,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }
    refused = await query_answer(
        _an_answering(
            db,
            _asking_for(wanted="add", rows=A_GROUP_ONE_OF_WHICH_IS_REFUSED),
            THE_GRAMMAR,
        )
    )
    return {
        **landed,
        "refused": refused,
        "after_the_refusal": await _what_the_table_holds(db),
    }


async def _people_named_bob(db: dict[str, Any]) -> dict[str, Any]:
    """A read narrowed by a value the caller wrote."""
    answered = await query_answer(
        _an_answering(
            db,
            _asking_for(wanted="all", where={"name": THE_NAME_THE_FEATURE_ASKS_FOR}),
            THE_GRAMMAR,
        )
    )
    return {
        "answered": answered,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _in_a_grammar_nothing_can_build(db: dict[str, Any]) -> dict[str, Any]:
    """A spelling with no builder. The lookup carries no fallback."""
    answered = await query_answer(
        _an_answering(db, _asking_for(wanted="all"), A_SPELLING_NOTHING_CAN_BUILD)
    )
    return {
        "answered": answered,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


WHAT_EACH_CALLER_ASKS: dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]] = {
    "a caller asks for every person": _every_person,
    "a caller asks for every person by the name of the class": _every_person_by_the_class_name,
    "a caller declares one new person": _one_new_person,
    "a caller declares three people together": _three_people_together,
    'a caller asks for people named ""': _people_named_bob,
    "a caller asks in a grammar nothing here can build": _in_a_grammar_nothing_can_build,
}


# =============================================================================
# reading a sentence that carries its own values
# =============================================================================

_QUOTED = re.compile(r'"[^"]*"')


def _without_its_values(said: str) -> str:
    return _QUOTED.sub('""', said.strip())


def _the_values_in(said: str) -> list[str]:
    return [found.strip('"') for found in _QUOTED.findall(said)]


# =============================================================================
# When: the caller asks
# =============================================================================


@when(parsers.re(r"^(?P<said>a caller .+)$"))
def _what_the_caller_asked(
    said: str,
    scenario: dict[str, object],
    tmp_path: pathlib.Path,
    request: pytest.FixtureRequest,
    require_postgresql: None,
) -> None:
    """One question, asked of all three engines, each on a database of its own."""
    work = WHAT_EACH_CALLER_ASKS[_without_its_values(said)]
    scenario["outcomes"] = asyncio.run(_against_every_engine(work, tmp_path, request.node.nodeid))


def _outcomes(scenario: dict[str, object]) -> dict[str, dict[str, Any]]:
    return scenario["outcomes"]  # type: ignore[return-value]


# =============================================================================
# Then: what came back, and what the database holds now
# =============================================================================


def _the_statement_ran(engine: str, outcome: dict[str, Any], _values: list[str]) -> None:
    answered = outcome["answered"]
    assert not _is_a_fault(answered), f"on {engine} the read came back {answered!r}"
    assert answered["statement"]["text"] != "", (
        f"on {engine} the record carries no statement, so nothing was compiled"
    )
    assert answered["rows"] == [{"id": row["id"], "name": row["name"]} for row in ALREADY_THERE], (
        f"on {engine} the driver handed back {answered['rows']!r} and the table "
        f"holds {outcome['in_the_table']!r}. A read that never reached the "
        "database would carry no rows at all"
    )
    assert outcome["room"]["arrivals"] == [], (
        f"on {engine} the room holds {outcome['room']['arrivals']!r} after a "
        "read, so the question was deposited instead of run"
    )


def _the_rows_are_shaped_as_the_model(
    engine: str, outcome: dict[str, Any], _values: list[str]
) -> None:
    answered = outcome["answered"]
    declared = [column["name"] for column in THE_MODEL["columns"]]
    assert isinstance(answered["answer"], list), (
        f"on {engine} the answer to a read is {answered['answer']!r}, and a "
        "read that answers now answers with rows"
    )
    assert answered["answer"] == [
        {"id": row["id"], "name": row["name"]} for row in ALREADY_THERE
    ], (
        f"on {engine} the answer is {answered['answer']!r} and the table holds "
        f"{outcome['in_the_table']!r}"
    )
    for shaped in answered["answer"]:
        assert sorted(shaped) == sorted(declared), (
            f"on {engine} a row came back carrying {sorted(shaped)} and the "
            f"model declares {sorted(declared)}"
        )


def _the_write_went_to_the_room(engine: str, outcome: dict[str, Any], _values: list[str]) -> None:
    answered = outcome["answered"]
    room = outcome["room"]
    assert not _is_a_fault(answered), f"on {engine} the write came back {answered!r}"
    assert len(room["arrivals"]) == 1, (
        f"on {engine} the room holds {room['arrivals']!r} after one write was "
        "declared, and a deposit puts exactly one write in it"
    )
    waiting = room["writes"][room["arrivals"][0]]
    assert waiting == answered["statement"], (
        f"on {engine} the room holds {waiting!r} and the record carries "
        f"{answered['statement']!r}, so what is waiting is not what was compiled"
    )
    assert outcome["in_the_table"] == [
        {"id": row["id"], "name": row["name"]} for row in ALREADY_THERE
    ], (
        f"on {engine} the table already holds {outcome['in_the_table']!r}. A "
        "deposit runs nothing, and the row reaches the engine later on a spare "
        "moment"
    )


def _a_ticket_came_back(engine: str, outcome: dict[str, Any], _values: list[str]) -> None:
    answered = outcome["answered"]
    room = outcome["room"]
    assert isinstance(answered["answer"], str), (
        f"on {engine} a deposited write answered with a "
        f"{type(answered['answer']).__name__} carrying {answered['answer']!r}, "
        "and a ticket is a string"
    )
    assert answered["answer"] in room["writes"], (
        f"on {engine} the caller holds {answered['answer']!r} and the room is "
        f"waiting on {sorted(room['writes'])}, so the ticket buys nothing"
    )


def _they_run_as_one_transaction(engine: str, outcome: dict[str, Any], _values: list[str]) -> None:
    answered = outcome["answered"]
    assert not _is_a_fault(answered), f"on {engine} the group came back {answered!r}"
    assert answered["answer"] == len(THREE_TOGETHER), (
        f"on {engine} a group of {len(THREE_TOGETHER)} rows answered "
        f"{answered['answer']!r}. A group answers with one number, not a ticket "
        "and not one answer per row"
    )
    assert outcome["in_the_table"] == [
        {"id": row["id"], "name": row["name"]} for row in [*ALREADY_THERE, *THREE_TOGETHER]
    ], f"on {engine} the group left {outcome['in_the_table']!r} in the table"

    assert _is_a_fault(outcome["refused"]), (
        f"on {engine} a group claiming a key that is taken came back "
        f"{outcome['refused']!r}, so this scenario is asking about a group no "
        "engine refuses"
    )
    assert outcome["after_the_refusal"] == outcome["in_the_table"], (
        f"on {engine} the refused group left {outcome['after_the_refusal']!r} "
        f"and the table held {outcome['in_the_table']!r} before it ran. The row "
        "before the refused one stayed, so the three were three writes rather "
        "than one transaction"
    )


def _nothing_was_deposited(engine: str, outcome: dict[str, Any], _values: list[str]) -> None:
    room = outcome["room"]
    assert room["arrivals"] == [] and room["writes"] == {}, (
        f"on {engine} the room holds {room!r} after a group ran. A group that "
        "went through the room is several independent writes, and the first "
        "stays landed when a later one fails"
    )
    assert not isinstance(outcome["answered"]["answer"], str), (
        f"on {engine} the group answered {outcome['answered']['answer']!r}, "
        "which is a ticket, and a group is the one write that does not take one"
    )


def _the_statement_carries_a_placeholder(
    engine: str, outcome: dict[str, Any], _values: list[str]
) -> None:
    """The sentence names no value, so the one the scenario asked for is read
    from the question this file put to the orchestrator."""
    answered = outcome["answered"]
    assert not _is_a_fault(answered), f"on {engine} the read came back {answered!r}"
    text = answered["statement"]["text"]
    assert THE_NAME_THE_FEATURE_ASKS_FOR not in text, (
        f"on {engine} the statement reads {text!r}, and the value the caller "
        f"wrote is in it. A value pasted into the text is a value the driver "
        "never bound"
    )
    assert answered["statement"]["parameters"] != {}, (
        f"on {engine} the statement reads {text!r} and binds nothing, so "
        "there is no placeholder for the driver to fill"
    )


def _the_value_came_back_beside_it(engine: str, outcome: dict[str, Any], values: list[str]) -> None:
    answered = outcome["answered"]
    bound = answered["statement"]["parameters"]
    assert values[0] in list(bound.values()), (
        f"on {engine} the statement binds {bound!r} and the caller wrote "
        f"{values[0]!r}, so the value never reached the driver"
    )
    assert answered["answer"] == [
        {"id": row["id"], "name": row["name"]} for row in ALREADY_THERE if row["name"] == values[0]
    ], (
        f"on {engine} the answer is {answered['answer']!r}. The bound value has "
        f"to narrow the read, or a placeholder that reached the driver did "
        "nothing"
    )


def _it_is_refused_naming_the_spelling(
    engine: str, outcome: dict[str, Any], _values: list[str]
) -> None:
    answered = outcome["answered"]
    assert _is_a_fault(answered), (
        f"on {engine} a question in {A_SPELLING_NOTHING_CAN_BUILD!r} came back "
        f"{answered!r}. A spelling with no builder has to be refused, never "
        "answered by whichever builder was nearest"
    )
    assert A_SPELLING_NOTHING_CAN_BUILD in answered["what_went_wrong"], (
        f"on {engine} the refusal says {answered['what_went_wrong']!r} and never "
        f"names {A_SPELLING_NOTHING_CAN_BUILD!r}, so a reader cannot tell which "
        "word arrived"
    )
    for spelled in THE_FOUR_SPELLINGS:
        assert spelled in answered["what_to_do_instead"], (
            f"on {engine} the refusal says {answered['what_to_do_instead']!r} "
            f"and leaves out {spelled!r}, which is one of the four a caller may "
            "ask in"
        )


def _nothing_reached_the_database(engine: str, outcome: dict[str, Any], _values: list[str]) -> None:
    assert outcome["in_the_table"] == [
        {"id": row["id"], "name": row["name"]} for row in ALREADY_THERE
    ], (
        f"on {engine} the table holds {outcome['in_the_table']!r} after a "
        "question that was refused before anything was compiled"
    )
    assert outcome["room"]["arrivals"] == [], (
        f"on {engine} the room holds {outcome['room']['arrivals']!r} after a "
        "refusal, so a question nobody could build was deposited anyway"
    )


WHAT_EACH_THEN_READS: dict[str, Callable[[str, dict[str, Any], list[str]], None]] = {
    "the statement ran against the database": _the_statement_ran,
    "the rows came back shaped as the model": _the_rows_are_shaped_as_the_model,
    "the write went to the room": _the_write_went_to_the_room,
    "a ticket came back": _a_ticket_came_back,
    "they run as one transaction": _they_run_as_one_transaction,
    "nothing was deposited": _nothing_was_deposited,
    "the statement carries a placeholder": _the_statement_carries_a_placeholder,
    '"" came back beside it for the driver to bind': _the_value_came_back_beside_it,
    "it is refused naming what arrived and the four spellings there are": (
        _it_is_refused_naming_the_spelling
    ),
    "nothing reached the database": _nothing_reached_the_database,
}


@then(parsers.re(r"^(?P<said>.+)$"))
def _what_must_be_true(said: str, scenario: dict[str, object]) -> None:
    """Eight sentences, one function over a table, asked of all three engines.

    IT SAYS HOW MANY IT ASKED, because a loop over nothing passes. Every check
    here runs inside the loop below, so an empty set of outcomes would satisfy
    this step without asking anything, and the scenario would read as green on a
    question that never ran.
    """
    outcomes = _outcomes(scenario)
    assert len(outcomes) == len(WHERE_EACH_ENGINES_DATABASE_GOES), (
        f"{said!r} was asked of {sorted(outcomes)}, and this file runs every "
        f"scenario against {sorted(WHERE_EACH_ENGINES_DATABASE_GOES)}. A check "
        "nobody ran is a check that passed."
    )
    for engine, outcome in outcomes.items():
        WHAT_EACH_THEN_READS[_without_its_values(said)](engine, outcome, _the_values_in(said))
