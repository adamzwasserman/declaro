"""Bind `tests/bdd/features/session/session_connect.feature`.

WHAT THE FEATURE ASKS. The handle comes back only after the database matches its
models, a database already matching is not touched, the library makes the crew,
the engine is decided from the target before anything opens, a destructive change
nobody declared stops the open, and the handle carries what everything downstream
reads.

EVERY BOUND SCENARIO RUNS ON A REAL SQLITE FILE. What these scenarios turn on is
whether a table is there when the handle goes back, and a stand-in that recorded
calls would pass while the database was still empty. The tables are read off the
file with a second connection after the open has finished.

THE MODELS ARE PYDANTIC CLASSES DECLARED INSIDE A STEP, NEVER AT MODULE LEVEL.
`reader fn schema_read_declared` walks every Pydantic class alive in the process,
so a class held at import time joins every other test's migration for the whole
run. Each scenario holds its own in its own record and lets go at the end.

THE CREW STOPS AND THE CONNECTIONS CLOSE INSIDE THE LOOP THAT OPENED THEM. A
drainer is an `asyncio` task and a task started in one loop cannot be stopped
from another, so one async function opens, reads back what the assertions need,
and shuts the whole thing down before any Then runs.

WHAT EACH SENTENCE SETS UP IS A TABLE, NOT A FUNCTION EACH. Nine Given sentences
differ only by which tables they put in the file and whether they can run at all.
Written as nine bodies they were nine copies of two lines, and the one that
drifted would have drifted silently.

SIX SCENARIOS ARE `xfail` AND EACH NAMES WHAT IS MISSING. A skip would say the
line was satisfied when it was not. Every Then under an `xfail` is written to
fail loudly on a real check, so deleting a guard turns the scenario red and names
the piece somebody has to build.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import tempfile
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.channel_pull import channel_pull
from declaro_persistum.channel_push import channel_push
from declaro_persistum.crew_stop import crew_stop
from declaro_persistum.session_close_connection import session_close_connection
from declaro_persistum.session_connect import (
    THE_CHANNEL_IN_ORDER,
    THE_LIVE_SCHEMA_READ,
    THE_ORDER,
    session_connect,
)
from declaro_persistum.session_open_channel import session_open_channel
from declaro_persistum.types import NOTHING
from tests.conftest import a_connecting, a_plan, a_report, a_schema, declaring

scenarios("../features/session/session_connect.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# WHAT THE DOOR READS OFF THE ENVIRONMENT AND HANDS OVER. `boundary_in fn
# session_open` declares that it reads DECLARO_CONNECTIONS and DECLARO_CREW_SIZE,
# so both numbers arrive already read. Handing nothing instead is what a
# deployment that set neither looks like, and every scenario here would then be
# testing that refusal rather than what it says it tests.
HOW_MANY_CONNECTIONS_THIS_DEPLOYMENT_ASKED_FOR = 4
HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR = 2

THE_TABLE_THE_MODELS_DECLARE = "people"


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and frozenset(piece) == THE_THREE_FACTS_IN_A_FAULT


def _what_the_door_hands_over(target: str, primary: object, token: object) -> dict:
    """The record `boundary_in fn session_open` builds, with nothing filled in.

    Built through `a_connecting` rather than written out here. `Connecting` has
    nineteen fields, and a step file writing its own is the thirteenth file
    doing what cost this suite two silent days when `Column` lost `facets`.

    THE PRIMARY AND THE TOKEN COME FROM THE SCENARIO, because a replicated open
    is the same door with two of the caller's four words filled in. Every local
    scenario hands nothing for both, which is what a caller opening a file does.
    """
    return a_connecting(
        target=target,
        shutdown="exit_immediately",
        primary=primary,
        token=token,
        connections_asked=HOW_MANY_CONNECTIONS_THIS_DEPLOYMENT_ASKED_FOR,
        crew_asked=HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR,
        engine="sqlite",
        copy_was_here=False,
        settings={},
        permitted=0,
        crew_size=0,
        writers=[],
        connections=[],
        channel=NOTHING,
        declared=declaring(models=[], views=[], dropping_tables=[], dropping_views=[]),
        live=a_schema(tables=[], views=[]),
        wanted_schema=a_schema(tables=[], views=[]),
        needs_migration=False,
        migration=a_report(plan=a_plan(operations=[], destroys=[]), applied=0),
        database=None,
    )


# --- what each sentence sets up ------------------------------------------------
#
# A ROW HOLDS ONLY WHAT ITS SENTENCE SETS, AND THE REST STAYS AS IT WAS. Two
# Given sentences in one scenario are two rows applied in turn, so a row naming
# `tables` when its sentence says nothing about tables wipes what the sentence
# before it put there. "No model asking to drop it" says nothing about tables,
# and a row carrying an empty list for it emptied the database the line above
# had just filled. Every row still carries `why_not`, because that is the one
# question asked of every sentence.

WHAT_EACH_GIVEN_SETS: dict[str, dict] = {
    "a database with no tables": {"why_not": "", "tables": []},
    "a database whose schema already matches the models": {
        "why_not": "",
        "tables": ["CREATE TABLE people (id INTEGER NOT NULL, name TEXT NOT NULL)"],
    },
    # `Model.dropping` is how a model asks to lose a column, and the Background's
    # model declares none. This sentence sets nothing up; it says what is absent.
    "no model asking to drop it": {"why_not": ""},
    "the engine refuses the thirtieth": {"why_not": ""},
    # THE TWO REPLICATED SENTENCES RUN AGAINST THIS SUITE'S OWN PRIMARY, named by
    # TEST_TURSO_PRIMARY_URL and made 2026-10-10. They were parked until then,
    # because the only primary on this machine was multicardz's staging database:
    # its tables no test model declares, and `fn difference` plans a drop for
    # every live table the models do not declare, so the open refused on
    # undeclared loss before anything these scenarios ask about happened.
    "a replicated database whose local copy is empty": {"why_not": "", "primary_holds": "people"},
    "a replicated database holding a table the primary never got": {
        "why_not": "",
        "local_only": True,
    },
    "a replicated database whose push will not land": {
        "why_not": (
            "Needs a push that fails on a channel that opened. A primary that is "
            "not there opens a channel and refuses at the push, which is this "
            "scenario, except that the push also carries the open's first and "
            "only proof of the channel: the refusal names the primary either "
            "way, so nothing here can tell a push that failed from a channel "
            "that was never good. Telling them apart means the primary going "
            "away between two calls."
        ),
    },
    "models declaring every kind of change a migration can plan": {
        "why_not": (
            "Needs one set of models that draws all twelve kinds in "
            "`OperationKind` out of `fn difference`, including a rename, a view "
            "and an enum. `tests/conftest.py` builds one operation at a time and "
            "nothing builds a declaration that reaches every kind."
        ),
    },
    "a plan of five operations, two of which the database already has": {
        "why_not": (
            "`fn difference` emits only what differs, so a plan of five where the "
            "database already holds two cannot come out of it. Either the plan is "
            "handed in from outside, which no door does, or the report counts "
            "statements the engine treated as no-ops, which nothing measures."
        ),
    },
    "a database whose disk fills partway through the migration": {
        "why_not": (
            "Needs a disk that fills on the tenth statement and not the first. A "
            "full filesystem cannot be asked for from inside the process, and a "
            "driver stand-in raising on the tenth call would be testing the "
            "stand-in's counter rather than the engine's answer."
        ),
    },
    "a migration that stops on an engine that cannot undo what already ran": {
        "why_not": (
            "Needs a Turso database and a migration whose second operation that "
            "engine refuses. `fn difference` builds the plan out of the models, "
            "so nothing here can put a failing operation in the middle of one."
        ),
    },
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


def _it_can_run(_why_not: str) -> bool:
    """Nothing is missing, so the scenario goes on. Answered rather than left."""
    return True


def _it_cannot_run_yet(why_not: str) -> bool:
    """`pytest.xfail` names the missing piece and ends the scenario here."""
    pytest.xfail(why_not)


_WHETHER_ANYTHING_IS_MISSING: dict[bool, Any] = {False: _it_can_run, True: _it_cannot_run_yet}


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> Iterator[dict]:
    """What a scenario carries, and the primary left as it was found.

    THE PRIMARY OUTLIVES THIS PROCESS, so a table a scenario put there is a
    table the next run meets. Whatever goes in `to_clean` comes off the primary
    at the end, through a copy of its own.
    """
    held = {
        "tmp": tmp_path,
        "models": [],
        "tables": [],
        "lost": "",
        "why_not": "",
        # WHAT A REPLICATED SENTENCE ASKS FOR: a table to put on the primary
        # before the open, or a table to put in the local copy and never push.
        "primary_holds": "",
        "local_only": False,
        "to_clean": [],
        # WHAT A CALLER OPENING A FILE HANDS FOR THE OTHER TWO WORDS. A
        # replicated scenario writes a primary and a token over these.
        "primary": NOTHING,
        "token": NOTHING,
        # THE TABLE A REPLICATED SCENARIO PUTS IN ITS COPY, and what a second
        # replica sees on the primary afterwards. Empty here so that pulling the
        # guard off a scenario that cannot run yet fails on the assertion and
        # names what is missing, rather than on a key nobody filled.
        "table": "",
        "on_the_primary": [],
        # `tables` IS WHAT GOES INTO THE FILE FIRST; `holds` IS WHAT IT HOLDS
        # AFTERWARDS, read back off the file by the When. Two words, because one
        # would have the open's answer overwriting the scenario's own setup.
        #
        # A SCENARIO THAT NAMES NO DATABASE STILL OPENS ONE. "The library makes
        # the crew" and "the handle carries what everything downstream reads"
        # say nothing about the database, so they open a fresh empty file and
        # the migration puts the declared table into it.
        "target": f"sqlite:///{tmp_path / 'opened.db'}",
    }
    yield held
    _take_off_the_primary(held)


def _the_suites_own_primary() -> tuple[str, str]:
    """The primary this suite replicates from, or a failure naming what is missing.

    IT IS NOT THE ONE `TEST_TURSO_URL` NAMES. That one is multicardz's staging
    central database, whose tables no test model declares, so an open against it
    plans a drop for every one of them and refuses before anything a scenario
    here asks about happens.
    """
    url = os.environ.get("TEST_TURSO_PRIMARY_URL", "")
    token = os.environ.get("TEST_TURSO_PRIMARY_AUTH_TOKEN", "")
    missing = [
        name
        for name, value in (
            ("TEST_TURSO_PRIMARY_URL", url),
            ("TEST_TURSO_PRIMARY_AUTH_TOKEN", token),
        )
        if not value
    ]
    assert not missing, (
        f"{', '.join(missing)} is not set in .env, and the replicated scenarios "
        f"run against a real primary of this suite's own. A skip would read as a "
        f"tested backend."
    )
    return url, token


async def _a_channel_at(folder: pathlib.Path, name: str) -> Any:
    """One channel of this copy's own, opened through the door that opens them."""
    url, token = _the_suites_own_primary()
    channel = await session_open_channel(str(folder / name), url, token)
    assert not _is_fault(channel), f"the channel did not open: {channel}"
    return channel


async def _put_it_on_the_primary(statements: tuple[str, ...]) -> None:
    """Run these on a copy of their own and send them up."""
    with tempfile.TemporaryDirectory() as made:
        channel = await _a_channel_at(pathlib.Path(made), "setup.db")
        cursor = channel.cursor()
        for statement in statements:
            await cursor.execute(statement)
        await channel.commit()
        assert await channel_push(channel) is None, "the setup push did not land"
        await channel.close()


# ALL THREE HAND BACK WHAT THE SCENARIO LEARNED, and the caller writes it down.
# The do-nothing row answers with an empty record rather than with nothing: a
# body that returns None reads as a row nobody wrote, and a row that updated the
# scenario itself would be a side effect the other rows could forget to make.


def _nothing_is_replicated(_scenario: dict) -> dict:
    """A local database, so there is no primary and no copy to arrange."""
    return {}


def _a_primary_already_holding_the_table(scenario: dict) -> dict:
    """The primary holds what the models declare, and this machine holds nothing.

    The table is spelled the way the migration would spell it, so a copy that
    pulled it is a copy that matches its models and plans no migration.
    """
    url, token = _the_suites_own_primary()
    table = scenario["primary_holds"]
    asyncio.run(
        _put_it_on_the_primary((f"CREATE TABLE {table} (id INTEGER NOT NULL, name TEXT NOT NULL)",))
    )
    return {
        "primary": url,
        "token": token,
        "table": table,
        "to_clean": [table],
        "target": f"libsql:///{scenario['tmp'] / 'cold' / 'copy.db'}",
    }


def _a_copy_holding_what_the_primary_never_got(scenario: dict) -> dict:
    """A commit made on this copy and never sent, which is the failure this is for.

    The copy is made and written through its own channel, which is then closed:
    one sync connection per replica, measured 2026-08-12, so the open cannot
    have a second one while this is holding the first.
    """
    url, token = _the_suites_own_primary()
    # THE TABLE THE MODELS DECLARE, AND IT HAS TO BE. A table no model declares
    # is a table the open plans a drop for, and it refuses the undeclared loss
    # before anything this scenario asks about can be read. That refusal is
    # right, and it is `a destructive change nobody declared stops the open`
    # above. What this scenario is about is the deploy that made its own table
    # and never got it up, so the table is the one the deploy declared.
    table = THE_TABLE_THE_MODELS_DECLARE
    folder = scenario["tmp"] / "warm"
    path = f"libsql:///{folder / 'copy.db'}"

    async def _write_it_locally() -> None:
        channel = await _a_channel_at(folder, "copy.db")
        cursor = channel.cursor()
        await cursor.execute(f"CREATE TABLE {table} (id INTEGER NOT NULL, name TEXT NOT NULL)")
        await cursor.execute(f"INSERT INTO {table} VALUES (1, 'never sent')")
        await channel.commit()
        await channel.close()

    asyncio.run(_write_it_locally())
    return {"primary": url, "token": token, "table": table, "to_clean": [table], "target": path}


# WHICH SETUP A SENTENCE ASKS FOR, read off what its row set. A fourth
# combination would be a sentence asking for both, which none does, and the read
# has no fallback so it would say so.
_HOW_THE_COPY_AND_THE_PRIMARY_START: dict[tuple[bool, bool], Any] = {
    (False, False): _nothing_is_replicated,
    (True, False): _a_primary_already_holding_the_table,
    (False, True): _a_copy_holding_what_the_primary_never_got,
}


def _take_off_the_primary(scenario: dict) -> None:
    """Leave the primary as it was found, through a copy of its own."""

    async def _dropped() -> None:
        with tempfile.TemporaryDirectory() as made:
            channel = await _a_channel_at(pathlib.Path(made), "cleanup.db")
            cursor = channel.cursor()
            for table in scenario["to_clean"]:
                await cursor.execute(f"DROP TABLE IF EXISTS {table}")
            await channel.commit()
            await channel_push(channel)
            await channel.close()

    for _ in scenario["to_clean"][:1]:
        asyncio.run(_dropped())


@given('models declaring a table "people" with columns id and name')
def _the_models(scenario: dict) -> None:
    from pydantic import BaseModel

    # honest-code-allow: L1.21.5 -- a host's model IS a Pydantic class.
    # `HOW-IT-SHOULD-WORK.md:34`: "You write Pydantic models. That is your only
    # description of the database." `reader fn schema_read_declared` reads
    # exactly these, so a fixture declaring anything else would be testing an
    # input the library never receives.
    class Person(BaseModel):
        __tablename__ = "people"
        id: int
        name: str

    scenario["models"].append(Person)


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_set_up(scenario: dict, sentence: str) -> None:
    scenario.update(WHAT_EACH_GIVEN_SETS[sentence])
    _WHETHER_ANYTHING_IS_MISSING[scenario["why_not"] != ""](scenario["why_not"])
    scenario.update(
        _HOW_THE_COPY_AND_THE_PRIMARY_START[
            (scenario["primary_holds"] != "", scenario["local_only"])
        ](scenario)
    )


@given(parsers.parse('a model that no longer declares the column "{column}"'))
def _a_column_no_model_declares(scenario: dict, column: str) -> None:
    """The column is in the database and in no model, which is a loss."""
    scenario["tables"] = [
        f"CREATE TABLE people (id INTEGER NOT NULL, name TEXT NOT NULL, {column} TEXT NOT NULL)"
    ]
    scenario["lost"] = column
    scenario["why_not"] = ""


@given(parsers.parse('models whose migration is thirty operations on "{engine}"'))
def _thirty_operations(scenario: dict, engine: str) -> None:
    scenario["engine_asked"] = engine
    pytest.xfail(
        "Needs a way to declare thirty models and make the thirtieth operation "
        "fail on each of the three engines. Nothing in this suite builds a "
        "migration of a named length, and nothing makes one operation fail "
        "without making the whole batch invalid."
    )


# --- opening it ------------------------------------------------------------------


def _the_scenarios_own(_named: str | None, scenario: dict) -> str:
    return str(scenario["target"])


def _the_one_in_the_sentence(named: str, _scenario: dict) -> str:
    return named


_WHICH_DATABASE_IS_OPENED: dict[bool, Any] = {
    True: _the_scenarios_own,
    False: _the_one_in_the_sentence,
}

# ONE SENTENCE IN FIVE SPELLINGS, AND ONE FUNCTION BEHIND THEM. "It", "the
# database" and "a local database" all mean the one this scenario set up; the
# other two name a target or an engine inside the sentence.
_HOW_A_CALLER_SAYS_IT = (
    r"^a caller opens (?:it|the database|a local database|"
    r'"(?P<named>[a-z]+://[^"]*)"|a "(?P<engine>[a-z]+)" database)$'
)


class _AChannelThatRecordsWhatRan:
    """What stands in for the primary being reachable.

    IT RECORDS WHETHER IT WAS ASKED, which is the one thing the scenario below
    wants to know: a copy that was already here has to catch up with the other
    services on the primary, and a machine that started with nothing was filled
    by the push and has nothing to catch up on. `pull` answers the way the
    driver's does, with nothing, so this is a probe and not a second opinion
    about what a pull does.
    """

    def __init__(self, ran: list[str]) -> None:
        self._ran = ran

    async def pull(self) -> None:
        self._ran.append("pull")


WHETHER_A_COPY_WAS_HERE: dict[str, bool] = {"already here": True, "not here yet": False}
WHAT_BEING_ASKED_LOOKS_LIKE: dict[str, list[str]] = {"asked": ["pull"], "left alone": []}


@given(parsers.parse("a replicated open whose copy was {state}"))
def _a_replicated_open(scenario: dict, state: str) -> None:
    ran: list[str] = []
    scenario["ran"] = ran
    scenario["record"] = {
        **_what_the_door_hands_over(str(scenario["target"]), "libsql://wherever", "a-token"),
        "channel": _AChannelThatRecordsWhatRan(ran),
        "copy_was_here": WHETHER_A_COPY_WAS_HERE[state],
    }


@when("the step that brings the primary down runs")
def _the_pull_step_runs(scenario: dict) -> None:
    """The step out of the order the open runs, not a copy of it."""
    scenario["answer"] = asyncio.run(dict(THE_CHANNEL_IN_ORDER)["pull"](scenario["record"]))


@then(parsers.parse("the primary was {reached}"))
def _whether_the_primary_was_reached(scenario: dict, reached: str) -> None:
    assert not _is_fault(scenario["answer"]), f"the step refused: {scenario['answer']}"
    assert scenario["ran"] == WHAT_BEING_ASKED_LOOKS_LIKE[reached], (
        f"the copy's own answer about the disk was "
        f"{scenario['record']['copy_was_here']} and the channel saw "
        f"{scenario['ran']}, where this start {reached} the primary"
    )


@when("the channel's three steps are read in the order they run")
def _where_the_channel_steps_sit(scenario: dict) -> None:
    """Where each of the three sits in the whole order.

    `THE_CHANNEL_IN_ORDER` is spliced into `THE_ORDER` rather than written into
    it, so these are the same three closures the open runs and their positions
    are read off the order itself.
    """
    scenario["where_each_step_sits"] = {
        word: THE_ORDER.index(step) for word, step in THE_CHANNEL_IN_ORDER
    }
    scenario["where_the_schema_is_read"] = THE_ORDER.index(THE_LIVE_SCHEMA_READ)


@when(parsers.re(_HOW_A_CALLER_SAYS_IT))
def _a_caller_opens_a_database(scenario: dict, named: str | None, engine: str | None) -> None:
    """The edge, and the only place in this file that touches a database.

    THREE LOOPS AND NOT ONE, AND THAT IS ASYNCIO'S RULE RATHER THAN A CHOICE. A
    drainer is a task and a task started in one loop cannot be stopped from
    another, so the open and its shutdown share the middle loop. Putting the
    tables in and reading them back open connections of their own and close
    them again, so each gets its own.
    """
    scenario["engine_asked"] = engine
    target = _WHICH_DATABASE_IS_OPENED[named is None](named, scenario)
    scheme = target.partition("://")[0]

    asyncio.run(_WHAT_GOES_IN_FIRST[scheme](scenario, target))
    scenario.update(asyncio.run(_open_and_shut(target, scenario["primary"], scenario["token"])))
    scenario.update(asyncio.run(_WHAT_IT_HOLDS_AFTERWARDS[scheme](target)))


async def _open_and_shut(target: str, primary: object, token: object) -> dict:
    """Open it, then stop every drainer and close every connection it opened.

    Both happen in one loop because a task started in one cannot be stopped
    from another. What comes back is plain data, so no Then needs a loop.
    """
    answer = await session_connect(  # type: ignore[arg-type]
        _what_the_door_hands_over(target, primary, token)
    )
    # HOW MANY DRAINERS WERE RUNNING, counted by whatever stopped them. Counted
    # afterwards it read zero on a crew that had been running fine, because
    # stopping a crew empties its list of drainers.
    return {"answer": answer, "running": await _shut_down_whatever_opened(answer)}


def _the_record_in(scenario: dict) -> dict:
    """What the open filled in, for a Then that has already read the answer.

    NOTHING HERE HANDS BACK "NO RECORD". Every Then that reaches for a field
    asserts the open was not refused first, in its own words, so the reader that
    used to answer with nothing for a refusal is gone and so is every caller
    that had to test what came back before using it.
    """
    return dict(scenario["answer"])


def _the_path_in(target: str) -> str:
    """Where the copy lives, out of the target, the way the doors read one."""
    return target.partition("://")[2].removeprefix("/")


async def _whatever_the_scenario_built(scenario: dict, target: str) -> int:
    """Put the tables a Given asked for into the file, before anything opens it."""
    import aiosqlite

    connection = await aiosqlite.connect(_the_path_in(target), isolation_level=None)
    for statement in scenario["tables"]:
        await connection.execute(statement)
    await connection.close()
    return len(scenario["tables"])


async def _what_the_database_holds_now(target: str) -> dict:
    """Asked of the database with a connection of its own, never of the record."""
    import aiosqlite

    connection = await aiosqlite.connect(_the_path_in(target), isolation_level=None)
    tables = await connection.execute_fetchall(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    )
    columns = await connection.execute_fetchall(
        f"PRAGMA table_info({THE_TABLE_THE_MODELS_DECLARE})"
    )
    await connection.close()
    return {"holds": [str(row[0]) for row in tables], "columns": [str(row[1]) for row in columns]}


async def _nothing_to_put_anywhere(_scenario: dict, _target: str) -> int:
    """No file here to put tables in, so none go in. It is still an answer."""
    return 0


# WHAT GOES INTO A DATABASE BEFORE THE OPEN, AND WHAT IS READ BACK AFTER IT, one
# row per scheme a scenario writes. A replicated scenario arranges its own copy
# and its own primary in its Given, so nothing goes in here for one, and what it
# holds afterwards is read with a connection of its own.
_WHAT_GOES_IN_FIRST: dict[str, Any] = {
    "sqlite": _whatever_the_scenario_built,
    "libsql": _nothing_to_put_anywhere,
    "postgresql": _nothing_to_put_anywhere,
}


async def _nothing_on_this_machine(_target: str) -> dict:
    """A target no driver on this machine reaches holds nothing to read back."""
    return {"holds": [], "columns": []}


async def _what_the_copy_holds_now(target: str) -> dict:
    """Asked of the replica on disk, with a connection of its own.

    IT IS A PLAIN CONNECTION AND NOT A CHANNEL, because the question is what this
    machine holds. A channel would be a second sync connection to the replica,
    which the driver refuses, and it would pull before answering.
    """
    import turso.aio

    connection = await turso.aio.connect(_the_path_in(target))
    cursor = connection.cursor()
    await cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    tables = [str(row[0]) for row in await cursor.fetchall()]
    await cursor.execute(f"PRAGMA table_info({THE_TABLE_THE_MODELS_DECLARE})")
    columns = [str(row[1]) for row in await cursor.fetchall()]
    await connection.close()
    return {"holds": tables, "columns": columns}


_WHAT_IT_HOLDS_AFTERWARDS: dict[str, Any] = {
    "sqlite": _what_the_database_holds_now,
    "libsql": _what_the_copy_holds_now,
    "postgresql": _nothing_on_this_machine,
}


# --- shutting it down again -------------------------------------------------------


async def _nothing_was_opened(_answer: object) -> int:
    """The open was refused before it built a handle, so no drainer ever ran."""
    return 0


async def _stop_and_close(answer: object) -> int:
    """Stop every drainer, close every connection, and say how many ran.

    The count is taken before the crew stops, because stopping it empties the
    list, and it is what a scenario reads to say the crew came back running.
    """
    handle = dict(answer)["database"]  # type: ignore[call-overload]
    running = len(handle["crew"]["drainers"])
    await crew_stop(handle["crew"])
    for connection in [*handle["writers"], *handle["connections"]]:
        await session_close_connection(connection)
    return running


_WHETHER_THERE_IS_ANYTHING_TO_SHUT: dict[bool, Any] = {
    True: _nothing_was_opened,
    False: _stop_and_close,
}


async def _shut_down_whatever_opened(answer: object) -> int:
    return await _WHETHER_THERE_IS_ANYTHING_TO_SHUT[_is_fault(answer)](answer)


def _the_words_of(answer: object) -> str:
    """Everything a fault says, so an assertion reads one string and not two."""
    return str(answer["what_went_wrong"]) + str(answer["what_to_do_instead"])  # type: ignore[index]


# --- what must be true afterwards ---------------------------------------------------


@then("the table exists before the handle is returned")
def _the_table_is_there(scenario: dict) -> None:
    """A handle handed back while the database is still behind its models is a
    handle whose first question fails on a column the models declare."""
    assert not _is_fault(scenario["answer"]), f"the open was refused: {scenario['answer']}"
    assert THE_TABLE_THE_MODELS_DECLARE in scenario["holds"], (
        f"the handle came back and the database holds {scenario['holds']}, so a "
        f"caller's first question fails on a table the models declare"
    )


@then("no statement changed it")
def _nothing_ran(scenario: dict) -> None:
    assert not _is_fault(scenario["answer"]), f"the open was refused: {scenario['answer']}"
    ran = _the_record_in(scenario)["migration"]

    assert ran["applied"] == 0, (
        f"the database already matched its models and the migration ran "
        f"{ran['applied']} changes against it"
    )
    assert ran["plan"]["operations"] == [], (
        f"nothing had to change and the plan holds {ran['plan']['operations']}"
    )


# THREE SENTENCES SAYING THE SAME THING, WHICH IS ONE ASSERTION OVER A TABLE.
# What differs is the words the failure is reported in, and the words are the
# rows.
WHAT_A_HANDLE_MEANS_HERE: dict[str, str] = {
    "the handle comes back": ("the open was refused, and a caller with no handle has no database"),
    "none came back refused as one this engine cannot write": (
        "a change a model declares came back with no spelling for this engine"
    ),
}


@then(parsers.re(_one_of(tuple(sorted(WHAT_A_HANDLE_MEANS_HERE, key=len, reverse=True)))))
def _it_was_not_refused(scenario: dict, sentence: str) -> None:
    assert not _is_fault(scenario["answer"]), (
        f"{WHAT_A_HANDLE_MEANS_HERE[sentence]}: {scenario['answer']}"
    )
    assert _the_record_in(scenario)["database"] is not None, (
        "the open answered without a handle in it"
    )


WHAT_A_REFUSAL_MEANS_HERE: dict[str, str] = {
    "the open is refused": (
        "a change destroying a column no model asked to lose was allowed through"
    ),
    "a fault comes back instead of a handle": (
        "the migration stopped and the open answered with a handle anyway"
    ),
    "nothing read it back off the connection": (
        "a target no driver could reach came back with a handle, so something "
        "decided the engine after opening rather than before"
    ),
}


@then(parsers.re(_one_of(tuple(sorted(WHAT_A_REFUSAL_MEANS_HERE, key=len, reverse=True)))))
def _it_was_refused(scenario: dict, sentence: str) -> None:
    assert _is_fault(scenario["answer"]), (
        f"{WHAT_A_REFUSAL_MEANS_HERE[sentence]}: {scenario['answer']}"
    )
    assert "database" not in scenario["answer"], "a refusal came back carrying a handle"


# WHAT EACH SENTENCE NEEDS TO FIND IN THE REFUSAL'S OWN WORDS. Four sentences,
# each looking for its own phrases in the one string a fault amounts to.
WHAT_EACH_REFUSAL_HAS_TO_SAY: dict[str, tuple[str, ...]] = {
    "the engine was decided from the target": ("postgres",),
    "it names the operation it stopped on and what the engine said": (
        "stopped on",
        "The engine said",
    ),
    "the fault says the database is part way through its migration": ("part way through",),
    "it tells the operator to look at the database by hand": ("by hand",),
    "the refusal says the local commits could not be sent": ("Local commits could not be sent",),
}


@then(parsers.re(_one_of(tuple(sorted(WHAT_EACH_REFUSAL_HAS_TO_SAY, key=len, reverse=True)))))
def _the_refusal_says_it(scenario: dict, sentence: str) -> None:
    said = _the_words_of(scenario["answer"])
    missing = [one for one in WHAT_EACH_REFUSAL_HAS_TO_SAY[sentence] if one not in said]

    assert missing == [], (
        f"the refusal never says {missing}, which is what an operator reading "
        f"it has to find: {said!r}"
    )


@then("the crew came with the handle")
def _the_crew_came_with_it(scenario: dict) -> None:
    crew = _the_record_in(scenario)["database"]["crew"]

    assert crew is not None, (
        "the handle carries no crew, so a write deposited against it waits for "
        "a drainer that does not exist"
    )
    assert crew["size"] == HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR, (
        f"the deployment asked for {HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR} "
        f"drainers and the crew came back sized {crew['size']}"
    )


@then("nobody had to make one")
def _nobody_had_to_make_one(scenario: dict) -> None:
    """A crew a caller had to build is a crew a caller can leave out.

    What a caller hands over is a number. The bells a drainer sleeps on, the
    list of arrivals and the tickets are made in here, and a caller who never
    heard of any of them still gets a crew that runs.
    """
    assert "crew" not in _what_the_door_hands_over(str(scenario["target"]), NOTHING, NOTHING), (
        "the caller hands a crew over, so a caller who left it out would get a "
        "database that takes writes and never runs them"
    )
    assert scenario["running"] == HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR, (
        f"the deployment asked for {HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR} "
        f"drainers and {scenario['running']} were running when the handle came "
        f"back, so a write deposited against this database would sit there"
    )


@then("the refusal names the column and what to declare to allow it")
def _the_refusal_names_the_column(scenario: dict) -> None:
    said = _the_words_of(scenario["answer"])

    assert scenario["lost"] in said, f"the refusal does not name the column it stopped on: {said!r}"
    assert "dropping" in said, (
        f"the refusal does not say what to declare to allow the loss, so an "
        f"operator is left to guess: {said!r}"
    )


@then("the column is still there")
def _the_column_survived(scenario: dict) -> None:
    assert scenario["lost"] in scenario["columns"], (
        f"the open was refused and the column went anyway. The table holds {scenario['columns']}."
    )


@then("the copy was filled from the primary before the handle was returned")
def _the_copy_was_filled_first(scenario: dict) -> None:
    """A cold copy holds no rows until it has pulled, and the migration reads the
    live schema off that copy. Reading it before the pull plans a migration that
    creates every table the primary already has."""
    assert not _is_fault(scenario["answer"]), f"the open was refused: {scenario['answer']}"
    assert scenario["holds"] != [], (
        "the handle came back and the local copy is still empty, so nothing "
        "pulled from the primary before the open finished"
    )
    assert _the_record_in(scenario)["migration"]["applied"] == 0, (
        f"the copy came back matching its primary and the migration still ran "
        f"{_the_record_in(scenario)['migration']['applied']} changes, which is a migration "
        f"planned against an empty database"
    )


@then("the push runs after the channel opens and before the pull")
def _the_push_sits_between_them(scenario: dict) -> None:
    """Nothing can be sent before the channel is open, and what is local has to
    go up before anything comes down on top of it."""
    sits = scenario["where_each_step_sits"]

    assert sits["open"] < sits["push"] < sits["pull"], (
        f"the open runs the channel at {sits['open']}, the push at "
        f"{sits['push']} and the pull at {sits['pull']}. A pull before the push "
        f"brings the primary's schema down onto local changes that never left it"
    )


@then("both run before the live schema is read")
def _they_run_before_the_schema_is_read(scenario: dict) -> None:
    """On a replicated database the live schema is the local copy. A copy holding
    changes the primary never got reads as a copy that matches its models, so the
    migration plans nothing and nothing sends them again."""
    sits = scenario["where_each_step_sits"]
    read_at = scenario["where_the_schema_is_read"]

    assert max(sits.values()) < read_at, (
        f"the live schema is read at {read_at} and the channel's steps run at "
        f"{sits}, so the schema is read off a copy that has not sent what it holds"
    )


async def _what_a_second_replica_sees(folder: pathlib.Path) -> list[str]:
    """What the primary holds, read through a copy that never saw the write.

    ASKING THE COPY THAT PUSHED WOULD ANSWER FROM THE LOCAL FILE and say nothing
    about what left the machine, which is the whole of what a push is for.
    """
    channel = await _a_channel_at(folder, "second.db")
    assert await channel_pull(channel) is None, "the second replica could not pull"
    cursor = channel.cursor()
    await cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    held = [str(row[0]) for row in await cursor.fetchall()]
    await channel.close()
    return held


@then("the primary has that table")
def _the_primary_has_that_table(scenario: dict) -> None:
    with tempfile.TemporaryDirectory() as made:
        scenario["on_the_primary"] = asyncio.run(_what_a_second_replica_sees(pathlib.Path(made)))

    assert not _is_fault(scenario["answer"]), f"the open was refused: {scenario['answer']}"
    assert scenario["table"] in scenario["on_the_primary"], (
        f"this copy held {scenario['table']!r} and a second replica pulling from "
        f"the primary sees {scenario['on_the_primary']}, so the open never sent it"
    )


@then("the handle carries its engine, its connections, its writers and its crew")
def _the_handle_carries_it_all(scenario: dict) -> None:
    assert not _is_fault(scenario["answer"]), f"the open was refused: {scenario['answer']}"
    handle = _the_record_in(scenario)["database"]

    assert handle["engine"] == "sqlite", (
        f"the handle names {handle['engine']!r}, and every later call reads that "
        f"word to pick a driver"
    )
    assert handle["connections"] != [], "the handle holds no connection to ask a question on"
    assert len(handle["writers"]) == HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR, (
        f"the crew has {HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR} drainers "
        f"and the handle holds {len(handle['writers'])} writers, so a drainer "
        f"would have to share a connection"
    )
    assert handle["crew"]["size"] == HOW_MANY_DRAINERS_THIS_DEPLOYMENT_ASKED_FOR, (
        f"the handle holds a crew sized {handle['crew']['size']}"
    )


@then(parsers.parse('the twenty-nine before it are "{survives}"'))
def _what_the_failure_left(scenario: dict, survives: str) -> None:
    """All three engines take the batch as one transaction, so nothing a stopped
    migration ran is there the next morning.

    THE TABLE HELD A `kept` ROW UNTIL 2026-10-10, for the reading that Turso
    commits each change as it makes it. Measured against pyturso that day and it
    is false, so the feature's Turso row says "undone" with the other two and the
    row that answered "kept" is gone. A feature row saying anything but "undone"
    now fails at this lookup, which is what the table is for.
    """
    held = {"undone": 0}[survives]

    assert _the_record_in(scenario)["migration"]["applied"] == held, (
        f"the thirtieth change failed on {scenario['engine_asked']} and the "
        f"report says {_the_record_in(scenario)['migration']['applied']} ran, where this engine "
        f"leaves the twenty-nine before it {survives}"
    )


@then("every change reached the database")
def _every_change_landed(scenario: dict) -> None:
    assert not _is_fault(scenario["answer"]), f"the open was refused: {scenario['answer']}"
    ran = _the_record_in(scenario)["migration"]
    planned = len(ran["plan"]["operations"])

    assert ran["applied"] == planned, f"the plan holds {planned} changes and {ran['applied']} ran"


@then("the report names all five operations it planned")
def _the_report_names_the_plan(scenario: dict) -> None:
    planned = _the_record_in(scenario)["migration"]["plan"]["operations"]

    assert len(planned) == 5, f"the report names {len(planned)} operations, not the five it planned"


@then("the report says three of them ran")
def _the_report_counts_what_ran(scenario: dict) -> None:
    assert _the_record_in(scenario)["migration"]["applied"] == 3, (
        f"the report says {_the_record_in(scenario)['migration']['applied']} ran, and a caller "
        f"reading it cannot tell how much of the plan the database took"
    )


@then("it says what to do instead")
def _it_says_what_to_do(scenario: dict) -> None:
    assert str(scenario["answer"]["what_to_do_instead"]).strip() != "", (
        "the fault leaves an operator with nothing to do next"
    )
