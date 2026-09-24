"""Bind `tests/bdd/features/lifecycle/lifecycle_turn.feature`.

WHAT THE FEATURE ASKS. A turn runs what the crew is holding, a turn with nothing
waiting syncs instead, and a turn that goes wrong comes back as a value rather
than ending the drainer that ran it.

EVERY SCENARIO RUNS ON A REAL SQLITE DATABASE, because what a turn does is
write, and the rows afterwards are where arrival order shows.

THE THIRD SCENARIO IS THE ONE THAT MATTERS MOST HERE. A drainer runs this door
forever, so an exception reaching the runtime ends that drainer for good: the
crew goes on holding its connection, every later write waits for a worker that
is not there, and shutdown waits for a task that already died. The writer is
closed under the turn, which is the simplest real way to make everything inside
it fail at once.

THE CHANNEL IS A STAND-IN FOR THE PRIMARY BEING REACHABLE, and what the second
scenario asks of it is only whether a pass ran.
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.lifecycle_turn import lifecycle_turn
from declaro_persistum.types import Crew, Database, Draining, Statement

scenarios("../features/lifecycle/lifecycle_turn.feature")


THE_TABLE = "turned"
THREE_NOTES = ("first", "second", "third")
THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path}


def _a_write(note: str) -> Statement:
    return {"text": f"INSERT INTO {THE_TABLE} (note) VALUES (?)", "parameters": {"p1": note}}


class _AChannelThatRecordsWhatRan:
    """The primary being reachable, and nothing more than that."""

    def __init__(self, ran: list[str]) -> None:
        self._ran = ran

    async def push(self) -> None:
        self._ran.append("push")

    async def pull(self) -> bool:
        self._ran.append("pull")
        return False


# WHAT EACH SENTENCE SETS UP: what is deposited, whether there is a channel, and
# whether the writer is closed under the turn. Three sentences, one table.
WHAT_EACH_GIVEN_SETS: dict[str, dict] = {
    "three writes deposited in a known order": {
        "notes": THREE_NOTES,
        "channel": False,
        "closed": False,
    },
    "a crew holding nothing and a database that replicates": {
        "notes": (),
        "channel": True,
        "closed": False,
    },
    "a database whose writer has been closed under it": {
        "notes": ("one",),
        "channel": False,
        "closed": True,
    },
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_set_up(scenario: dict, sentence: str) -> None:
    scenario.update(WHAT_EACH_GIVEN_SETS[sentence])


@when("a turn arrives")
def _a_turn_arrives(scenario: dict) -> None:
    scenario.update(asyncio.run(_one_turn(scenario)))


async def _one_turn(scenario: dict) -> dict:
    import aiosqlite

    crew: Crew = crew_make()
    ran: list[str] = []
    path = scenario["tmp"] / "turn.db"
    writer = await aiosqlite.connect(path, isolation_level=None)
    await writer.execute(f"CREATE TABLE {THE_TABLE} (id integer primary key, note text)")
    db = Database(
        target=f"sqlite:///{path}",
        engine="sqlite",
        connections=[],
        writers=[writer],  # type: ignore[list-item]
        channel={True: _AChannelThatRecordsWhatRan(ran), False: None}[scenario["channel"]],  # type: ignore[typeddict-item]
        crew=crew,
        shutdown="exit_immediately",
    )
    for note in scenario["notes"]:
        assert isinstance(crew_deposit(crew, _a_write(note)), str)

    await _WHETHER_THE_WRITER_IS_CLOSED[scenario["closed"]](writer)
    raised = None
    try:
        answer = await lifecycle_turn(Draining(db=db, writer=writer, stop=crew["stop"], idle=0.0))
    except BaseException as came_out:  # noqa: BLE001
        answer, raised = None, came_out
    rows = await _rows_in(writer, scenario["closed"])
    await _WHETHER_THE_WRITER_IS_CLOSED[not scenario["closed"]](writer)
    return {"answer": answer, "raised": raised, "rows": rows, "sync": ran}


async def _close_it(writer) -> None:
    await writer.close()


async def _leave_it_open(_writer) -> None:
    return None


_WHETHER_THE_WRITER_IS_CLOSED = {True: _close_it, False: _leave_it_open}


async def _rows_in(writer, closed: bool) -> list[tuple]:
    return await _HOW_TO_READ_BACK[closed](writer)


async def _read_nothing(_writer) -> list[tuple]:
    """A closed writer has nothing to read back, and that is a real answer."""
    return []


async def _read_the_rows(writer) -> list[tuple]:
    cursor = await writer.execute(f"SELECT note FROM {THE_TABLE} ORDER BY id")
    return list(await cursor.fetchall())


_HOW_TO_READ_BACK = {True: _read_nothing, False: _read_the_rows}


@then("they ran in that order")
def _they_ran_in_order(scenario: dict) -> None:
    assert [row[0] for row in scenario["rows"]] == list(THREE_NOTES), (
        f"the table holds {scenario['rows']!r} and the writes arrived in {list(THREE_NOTES)}"
    )


@then("the sync pass ran")
def _the_sync_ran(scenario: dict) -> None:
    assert scenario["sync"] == ["push", "pull"], (
        f"a turn with nothing waiting ran {scenario['sync']} over the channel, "
        f"and a free turn is what shutdown relies on having spent syncing"
    )


@then("a fault comes back and nothing was raised")
def _a_fault_came_back(scenario: dict) -> None:
    """A drainer runs this forever, so an exception here ends it for good."""
    assert scenario["raised"] is None, (
        f"the turn raised {scenario['raised']!r} out to the runtime, which ends "
        f"this drainer: the crew goes on holding its connection, every later "
        f"write waits for a worker that is not there, and shutdown waits for a "
        f"task that already died"
    )
    answer = scenario["answer"]
    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a turn that went wrong answered {answer!r}, and a door answers with a Fault in the return"
    )
