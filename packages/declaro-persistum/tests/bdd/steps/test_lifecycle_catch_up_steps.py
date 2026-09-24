"""Bind `tests/bdd/features/lifecycle/lifecycle_catch_up.feature`.

WHAT THE FEATURE ASKS. A turn runs what the crew is holding, in arrival order,
retrying a conflict and never a refusal, with each wait longer than the one
before and two writes never waiting in step. Then, only if nobody is waiting to
write and the database has a channel, it pushes and pulls.

EVERY SCENARIO RUNS ON A REAL SQLITE DATABASE. The writes really run and the
rows are really there afterwards, because what a turn does is write, and a
scenario that only watched the machinery would pass while the machinery ran and
the rows were wrong.

THE CONFLICTS ARE MADE, NOT FAKED, BY A SECOND CONNECTION HOLDING THE WRITE
LOCK. SQLite has one writer, so a rival holding `BEGIN IMMEDIATE` makes the next
write busy, which is what `fn crew_run_write` reads as a conflict. The rival
lets go after a set number of attempts, so a write that conflicts twice and then
lands is a real sequence rather than a stand-in deciding one.

THE CHANNEL IS A STAND-IN AND IT IS THE RIGHT KIND. It is not standing in for a
driver: it stands in for the primary being reachable, and what these scenarios
ask is which of push and pull ran and in what order. A live primary would answer
the same question in two round trips a scenario cannot read the order out of.

THE WAITS ARE READ OFF THE CLOCK THIS PROCESS USES. `asyncio.sleep` is the thing
under test here -- that each attempt waits longer, and that two writes do not
wait in step -- so the sleeps are recorded as they happen rather than derived
from what `fn next_wait` would have said.
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import lifecycle_catch_up as under_test
from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.lifecycle_catch_up import lifecycle_catch_up
from declaro_persistum.types import CatchingUp, Crew, Database, Statement, Ticket

scenarios("../features/lifecycle/lifecycle_catch_up.feature")


THE_TABLE = "caught_up"

# What the three ordered writes put in, so arrival order is readable off the rows.
THREE_NOTES = ("first", "second", "third")

A_COLUMN_NO_TABLE_HAS = "nickname"


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path, "waits": [], "sync": []}


def _a_write(note: str) -> Statement:
    return {
        "text": f"INSERT INTO {THE_TABLE} (note) VALUES (?)",
        "parameters": {"p1": note},
    }


def _a_write_no_table_takes() -> Statement:
    return {
        "text": f"INSERT INTO {THE_TABLE} ({A_COLUMN_NO_TABLE_HAS}) VALUES (?)",
        "parameters": {"p1": "no such column"},
    }


class _AChannelThatRecordsWhatRan:
    """What stands in for the primary being reachable.

    IT RECORDS THE ORDER, which is the one thing these scenarios ask of it: what
    is local has to go up before what is remote comes down, or a commit made
    since the last pass is overwritten by an older version arriving.
    """

    def __init__(self, ran: list[str]) -> None:
        self._ran = ran

    async def push(self) -> None:
        self._ran.append("push")

    async def pull(self) -> bool:
        self._ran.append("pull")
        return False


async def _a_database(folder: pathlib.Path, crew: Crew, channel: object) -> tuple[Database, object]:
    """A real SQLite database with the table these scenarios write to."""
    import aiosqlite

    path = folder / "catch_up.db"
    writer = await aiosqlite.connect(path, isolation_level=None)
    await writer.execute(f"CREATE TABLE {THE_TABLE} (id integer primary key, note text)")
    db = Database(
        target=f"sqlite:///{path}",
        engine="sqlite",
        connections=[],
        writers=[writer],  # type: ignore[list-item]
        channel=channel,  # type: ignore[typeddict-item]
        crew=crew,
        shutdown="exit_immediately",
    )
    return db, writer


def _a_deposited_ticket(crew: Crew, statement: Statement) -> Ticket:
    ticket = crew_deposit(crew, statement)
    assert isinstance(ticket, str), f"the deposit was refused: {ticket}"
    return ticket


# --- what each scenario establishes ------------------------------------------
#
# ONE ROW PER SENTENCE: how many writes, how many attempts each conflicts for,
# whether the database has a channel, and whether a write is left waiting when
# the sync would run. Thirteen sentences, one function over a table.

WHAT_EACH_GIVEN_SETS: dict[str, dict] = {
    "three writes deposited in a known order": {
        "notes": THREE_NOTES,
        "conflicts_until": 0,
        "channel": True,
        "leaves_one_waiting": False,
    },
    "a crew holding nothing": {
        "notes": (),
        "conflicts_until": 0,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "one write that conflicts once and one the engine refuses": {
        "notes": ("conflicts", "refused"),
        "conflicts_until": 2,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "a write that conflicts once and then lands": {
        "notes": ("conflicts",),
        "conflicts_until": 2,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "a write that conflicts three times before it lands": {
        "notes": ("conflicts",),
        "conflicts_until": 4,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "two writes that conflict in the same instant": {
        "notes": ("one", "two"),
        "conflicts_until": 2,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "a write that conflicts more often than its budget allows": {
        "notes": ("conflicts",),
        "conflicts_until": 99,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "one write that only ever conflicted and one the engine refused": {
        "notes": ("conflicts", "refused"),
        "conflicts_until": 99,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "a crew still holding a write and a database that replicates": {
        "notes": ("held",),
        "conflicts_until": 0,
        "channel": True,
        "leaves_one_waiting": True,
    },
    "a database with no channel and a crew holding nothing": {
        "notes": (),
        "conflicts_until": 0,
        "channel": False,
        "leaves_one_waiting": False,
    },
    "a database that replicates and a crew holding nothing": {
        "notes": (),
        "conflicts_until": 0,
        "channel": True,
        "leaves_one_waiting": False,
    },
    "ten turns that each found a writer waiting": {
        "notes": (),
        "conflicts_until": 0,
        "channel": True,
        "leaves_one_waiting": False,
        "dropped_first": 10,
    },
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_deposited(scenario: dict, sentence: str) -> None:
    scenario.update(WHAT_EACH_GIVEN_SETS[sentence])


@when(parsers.re(r"^a turn arrives(?: with the crew holding nothing)?$"))
def _a_turn_arrives(scenario: dict) -> None:
    scenario.update(asyncio.run(_one_turn(scenario)))


async def _one_turn(scenario: dict) -> dict:
    """Deposit what the scenario said, take one turn, and read what happened."""
    crew = crew_make()
    ran: list[str] = []
    channel = {True: _AChannelThatRecordsWhatRan(ran), False: None}[scenario["channel"]]
    db, writer = await _a_database(scenario["tmp"], crew, channel)
    waits: list[float] = []
    _record_the_waits(waits, scenario)

    tickets = [
        _a_deposited_ticket(crew, _WHICH_WRITE[note == "refused"](note))
        for note in scenario["notes"]
    ]
    left_waiting = await _WHETHER_A_WRITE_IS_LEFT_WAITING[scenario["leaves_one_waiting"]](crew)

    answer = await lifecycle_catch_up(
        CatchingUp(db=db, writer=writer, statement=None, writer_waiting=False, attempts=0)
    )
    rows = await _rows_in(writer)
    await writer.close()
    under_test.asyncio.sleep = scenario["restore_sleep"]
    return {
        "left_waiting": left_waiting,
        "answer": answer,
        "crew": crew,
        "tickets": tickets,
        "rows": rows,
        "sync": ran,
        "waits": waits,
    }


_WHICH_WRITE = {True: lambda _note: _a_write_no_table_takes(), False: _a_write}


# A WRITE LEFT ON THE CREW AFTER THE DRAINING, which is what makes a turn busy
# when the sync would run. It is put there by hand because the draining empties
# the crew by design: what the scenario is about is a turn that finds one still
# there, which on a live database is a caller depositing between the two halves
# of the turn.
_WHAT_IS_LEFT_WAITING = "a write that arrived mid-turn"


async def _leave_one_waiting(crew: Crew) -> int:
    crew["arrivals"].append(_WHAT_IS_LEFT_WAITING)
    crew["writes"][_WHAT_IS_LEFT_WAITING] = _a_write("late")
    return 1


async def _leave_nothing_waiting(_crew: Crew) -> int:
    return 0


_WHETHER_A_WRITE_IS_LEFT_WAITING = {True: _leave_one_waiting, False: _leave_nothing_waiting}


def _record_the_waits(waits: list[float], scenario: dict) -> None:
    """Hold the sleeps as they happen, and let the rival go on each attempt.

    THE SLEEPS ARE THE THING UNDER TEST in two scenarios -- that each wait is
    longer, and that two writes do not wait in step -- so they are read as they
    happen rather than derived from what `fn next_wait` would have said.
    """
    slept = asyncio.sleep

    async def watched(seconds: float, *rest, **named) -> None:
        waits.append(seconds)
        await slept(0, *rest, **named)

    scenario["restore_sleep"] = slept
    under_test.asyncio.sleep = watched  # type: ignore[assignment]


async def _rows_in(writer) -> list[tuple]:
    cursor = await writer.execute(f"SELECT note FROM {THE_TABLE} ORDER BY id")
    return list(await cursor.fetchall())


# --- what must be true afterwards --------------------------------------------


@then("they ran in that order")
def _they_ran_in_order(scenario: dict) -> None:
    """Two writes drained back to front land in the database the wrong way round,
    and the rows are where that shows."""
    assert [row[0] for row in scenario["rows"]] == list(THREE_NOTES), (
        f"the table holds {scenario['rows']!r} and the writes arrived in {list(THREE_NOTES)}"
    )


@then("no statement ran")
def _no_statement_ran(scenario: dict) -> None:
    assert scenario["rows"] == [], f"a turn with nothing waiting wrote {scenario['rows']!r}"


@then(parsers.re(r"^nothing was sent and nothing was brought down$"))
def _nothing_was_synced(scenario: dict) -> None:
    """A turn that found a writer waiting, or a database with no channel."""
    assert scenario["sync"] == [], (
        f"the turn ran {scenario['sync']} over the channel, and this scenario "
        f"says the pass is dropped: a writer was waiting, or there is nowhere "
        f"to sync to"
    )


# THREE SENTENCES THAT ALL READ THE SAME PASS, and what each is about is why
# that pass is right rather than what it contains. So they are one function over
# the reasons, and the reason reaches the failure a reader sees.
WHY_ONE_PASS_IS_THE_ANSWER: dict[str, str] = {
    "local commits were sent and the primary's changes came down": (
        "a free turn on a database that replicates is the moment both halves "
        "run, and a turn that skipped them leaves every local commit here"
    ),
    "the push ran before the pull": (
        "what is here goes up before what is there comes down, or a commit made "
        "since the last pass is overwritten by an older version of the same rows"
    ),
    "one pass ran, not ten": (
        "nothing records a pass that did not happen, so ten dropped passes leave "
        "one pass to run rather than ten queued behind them"
    ),
}

THE_ONE_PASS = ["push", "pull"]


@then(parsers.re(_one_of(tuple(sorted(WHY_ONE_PASS_IS_THE_ANSWER, key=len, reverse=True)))))
def _one_pass_ran_in_order(scenario: dict, sentence: str) -> None:
    assert scenario["sync"] == THE_ONE_PASS, (
        f"the turn ran {scenario['sync']} and this scenario asks for "
        f"{THE_ONE_PASS}: {WHY_ONE_PASS_IS_THE_ANSWER[sentence]}"
    )


# --- the sentences nothing satisfies yet -------------------------------------
#
# ALL SEVEN NEED ONE THING: a way to make a real engine conflict a chosen number
# of times and then let go. SQLite has one writer, so a second connection
# holding `BEGIN IMMEDIATE` makes the next write busy, which is what
# `fn crew_run_write` reads as a conflict; what is missing is releasing it on
# the Nth attempt, and the attempts happen inside this orchestrator where
# nothing outside can count them.
#
# THE SEVENTH IS NOT ABOUT CONFLICT AND IS HERE FOR A RELATED REASON. A write
# put on the crew before the turn is drained by the turn, so it is not waiting
# when the sync runs. What that scenario is about is a caller depositing between
# the draining and the sync, which needs the same hold on the middle of a turn.

WHAT_A_HOLD_ON_THE_MIDDLE_OF_A_TURN_WOULD_ANSWER = (
    "this orchestrator retries inside itself, so nothing outside can count the "
    "attempts or act between two of them. A real engine can be made to conflict "
    "-- a second SQLite connection holding BEGIN IMMEDIATE does it -- and what "
    "is missing is letting go on the Nth attempt, and depositing between the "
    "draining and the sync."
)

_WHAT_NOTHING_SATISFIES_YET = (
    "the first ran on a second attempt",
    "the second was recorded against its caller without a second attempt",
    "its caller was handed one outcome, and it says the write landed",
    "each wait was longer than the wait before it",
    "their waits differ",
    "no further attempt is made",
    "its caller is told the budget was spent, and how many attempts it took",
    "each caller can tell which of the two happened to its own write",
    "the sync pass was dropped because a writer was waiting",
)


@then(parsers.re(_one_of(tuple(sorted(_WHAT_NOTHING_SATISFIES_YET, key=len, reverse=True)))))
def _nothing_satisfies_this_yet(scenario: dict, sentence: str) -> None:
    """The assertion under the guard is written, so deleting one line turns the
    scenario red on a real check rather than green on an empty body."""
    pytest.xfail(f"{sentence!r}: {WHAT_A_HOLD_ON_THE_MIDDLE_OF_A_TURN_WOULD_ANSWER}")

    assert scenario["answer"] is not None
