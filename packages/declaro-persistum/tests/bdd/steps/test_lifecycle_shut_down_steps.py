"""Bind `tests/bdd/features/lifecycle/lifecycle_shut_down.feature`.

WHAT THE FEATURE ASKS. The crew stops before anything is pushed, the channel is
taken whatever is waiting, a refusal is retried, progress is said out loud, a
local database has nothing to push, every connection closes, and a channel that
never takes it is reported rather than waited on forever.

EVERY SCENARIO RUNS ON A REAL SQLITE DATABASE, so the connections that close are
real ones and "is it shut" is asked by running a statement on it.

THE CHANNEL IS A STAND-IN, AND IT RECORDS WHEN IT WAS ASKED. What these
scenarios turn on is order -- did the crew stop before the push -- and count --
how many attempts a refusing channel drew. A live primary answers neither of
those in a way a scenario can read.

THE LOG IS READ THROUGH `caplog`, WHICH IS THE REAL LOGGER. "It says what it is
doing while it does it" is the whole of one scenario, and a stand-in that
recorded calls would pass while nothing an operator can read was written.
"""

from __future__ import annotations

import asyncio
import logging
import pathlib

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.lifecycle_shut_down import lifecycle_shut_down
from declaro_persistum.types import Crew, Database, ShuttingDown

scenarios("../features/lifecycle/lifecycle_shut_down.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

WHAT_A_REFUSING_CHANNEL_SAYS = "the primary would not take it"


class _AChannelThatRecordsWhenItWasAsked:
    """The primary, standing in so a scenario can read order and count.

    `refuses_first` IS HOW MANY ATTEMPTS IT TURNS DOWN before taking one, so a
    channel that refuses twice and then takes it is a real sequence rather than
    a double deciding one. `refuses_every` never takes it at all.
    """

    def __init__(self, asked: list, refuses_first: int, refuses_every: bool) -> None:
        self.asked = asked
        self._refuses_first = refuses_first
        self._refuses_every = refuses_every
        self.attempts = 0
        self.taken = False

    async def push(self) -> None:
        self.attempts = self.attempts + 1
        self.asked.append(("push", self.attempts))
        self.taken = _WHETHER_IT_TAKES_IT[
            self._refuses_every or self.attempts <= self._refuses_first
        ]()

    async def pull(self) -> bool:
        return False


# BOTH HALVES SAY WHETHER THE PRIMARY TOOK IT, so neither is a call made for
# its effect. The half that refuses raises, which is what a driver does, and the
# half that takes it answers that it did.
def _turn_it_down() -> bool:
    raise OSError(WHAT_A_REFUSING_CHANNEL_SAYS)


def _take_it() -> bool:
    return True


_WHETHER_IT_TAKES_IT = {True: _turn_it_down, False: _take_it}


async def _a_drainer_that_records_when_it_stopped(crew: Crew, asked: list):
    """One drainer, waiting on the stop and writing down when it finished."""

    async def drainer() -> None:
        await crew["stop"].wait()
        asked.append(("drainer stopped", len(asked)))

    return asyncio.ensure_future(drainer())


# WHAT EACH SENTENCE SETS UP: how many attempts the channel turns down, whether
# it ever takes one, whether the database has a channel at all, whether a crew is
# running, whether a write is left waiting, and how many databases there are.
WHAT_EACH_GIVEN_SETS: dict[str, dict] = {
    "a database that replicates, with its crew running": {
        "channel": True,
        "refuses_first": 0,
        "refuses_every": False,
        "drainers": 1,
        "waiting": False,
        "databases": 1,
    },
    "a database that replicates, with a write still waiting": {
        "channel": True,
        "refuses_first": 0,
        "refuses_every": False,
        "drainers": 0,
        "waiting": True,
        "databases": 1,
    },
    "a channel refusing the first two attempts": {
        "channel": True,
        "refuses_first": 2,
        "refuses_every": False,
        "drainers": 0,
        "waiting": False,
        "databases": 1,
    },
    "a database that replicates": {
        "channel": True,
        "refuses_first": 0,
        "refuses_every": False,
        "drainers": 0,
        "waiting": False,
        "databases": 1,
    },
    "a database with no channel": {
        "channel": False,
        "refuses_first": 0,
        "refuses_every": False,
        "drainers": 0,
        "waiting": False,
        "databases": 1,
    },
    "two databases, one that replicates and one that does not": {
        "channel": True,
        "refuses_first": 0,
        "refuses_every": False,
        "drainers": 0,
        "waiting": False,
        "databases": 2,
    },
    "a channel that refuses every attempt": {
        "channel": True,
        "refuses_first": 0,
        "refuses_every": True,
        "drainers": 0,
        "waiting": False,
        "databases": 1,
    },
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@pytest.fixture
def scenario(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> dict:
    caplog.set_level(logging.INFO)
    return {"tmp": tmp_path, "log": caplog}


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_set_up(scenario: dict, sentence: str) -> None:
    scenario.update(WHAT_EACH_GIVEN_SETS[sentence])


@when("the process is shutting down")
def _the_process_shuts_down(scenario: dict) -> None:
    scenario.update(asyncio.run(_a_shutdown(scenario)))


async def _one_database(scenario: dict, n: int, asked: list, channel: object) -> Database:
    import aiosqlite

    crew: Crew = crew_make()
    path = scenario["tmp"] / f"db{n}.db"
    writer = await aiosqlite.connect(path, isolation_level=None)
    reader = await aiosqlite.connect(path, isolation_level=None)
    crew["drainers"] = [
        await _a_drainer_that_records_when_it_stopped(crew, asked)
        for _ in range(scenario["drainers"])
    ]
    _WHETHER_A_WRITE_IS_WAITING[scenario["waiting"]](crew)
    return Database(
        target=f"sqlite:///{path}",
        engine="sqlite",
        connections=[reader],  # type: ignore[list-item]
        writers=[writer],  # type: ignore[list-item]
        channel=channel,  # type: ignore[typeddict-item]
        crew=crew,
        shutdown="replicate_then_exit",
    )


def _leave_one_waiting(crew: Crew) -> int:
    crew["arrivals"].append("still waiting")
    return 1


def _leave_nothing_waiting(_crew: Crew) -> int:
    return 0


_WHETHER_A_WRITE_IS_WAITING = {True: _leave_one_waiting, False: _leave_nothing_waiting}


async def _a_shutdown(scenario: dict) -> dict:
    """Build what the scenario said, shut it down, and read what happened."""
    asked: list = []
    channel = {
        True: _AChannelThatRecordsWhenItWasAsked(
            asked, scenario["refuses_first"], scenario["refuses_every"]
        ),
        False: None,
    }[scenario["channel"]]
    databases = [
        await _one_database(scenario, n, asked, {0: channel, 1: None}[n])
        for n in range(scenario["databases"])
    ]
    answer = await lifecycle_shut_down(
        ShuttingDown(databases=databases, started_at=0.0, attempts=0)
    )
    return {
        "answer": answer,
        "databases": databases,
        "asked": asked,
        "channel": channel,
        "still_open": await _which_are_still_open(databases),
    }


async def _which_are_still_open(databases: list[Database]) -> list[str]:
    """Asked of the driver, not of a flag: a closed connection refuses work."""
    open_still = []
    for db in databases:
        for connection in [*db["writers"], *db["connections"]]:
            try:
                await connection.execute("SELECT 1")
                open_still.append(db["target"])
            except Exception:  # noqa: BLE001, PERF203
                pass
    return open_still


# --- what must be true afterwards --------------------------------------------


@then("every drainer had finished before the push ran")
def _the_crew_stopped_first(scenario: dict) -> None:
    """A push before the crew stopped sends whatever happened to be committed
    and leaves the rest, which is exactly the writes a shutdown is racing."""
    order = [what for what, _n in scenario["asked"]]

    assert "drainer stopped" in order, (
        f"no drainer ever finished, so the crew was not stopped: {order}"
    )
    assert order.index("drainer stopped") < order.index("push"), (
        f"the push ran before the crew stopped: {order}. A write still landing "
        f"when the push runs is a write that never leaves the machine."
    )


@then("the push ran anyway, because politeness at exit is data loss")
def _it_took_the_channel(scenario: dict) -> None:
    assert scenario["channel"].attempts >= 1, (
        "a write was still waiting and shutdown did not push. Everywhere else a "
        "round trip yields to real work; here the disk does not survive the "
        "process, so yielding is the data going with it."
    )


@then("it tried again and the commits went up")
def _it_tried_again(scenario: dict) -> None:
    assert scenario["channel"].attempts == 3, (
        f"the channel turned down two attempts and was asked "
        f"{scenario['channel'].attempts} times: giving up early is the same as "
        f"not trying, because the commits are equally gone either way"
    )
    assert not isinstance(scenario["answer"], dict) or "kind" not in scenario["answer"], (
        f"the push landed on the third attempt and shutdown answered {scenario['answer']!r}"
    )


@then("every line names the database and how long it has taken")
def _it_says_what_it_is_doing(scenario: dict) -> None:
    """A shutdown that takes thirty seconds and says nothing is
    indistinguishable from one that hung."""
    written = scenario["log"].text
    named = scenario["databases"][0]["target"]

    assert named in written, (
        f"the log never names the database being shut down, so an operator "
        f"watching two exit at once cannot tell which is stuck: {written!r}"
    )
    assert "elapsed" in written, (
        f"the log never says how long it has taken, which is the difference "
        f"between slow and hung: {written!r}"
    )


@then("nothing was pushed, and its connections were closed")
def _a_local_database(scenario: dict) -> None:
    assert scenario["asked"] == [], (
        f"a database with no channel asked one for something: {scenario['asked']}"
    )
    assert scenario["still_open"] == [], (
        f"{scenario['still_open']} still answer statements after shutdown, and "
        f"every connection is an operating system thread"
    )


@then("every connection either of them held is shut")
def _every_connection_is_shut(scenario: dict) -> None:
    assert scenario["still_open"] == [], (
        f"{scenario['still_open']} still answer statements after shutdown"
    )


@then("a fault comes back naming the database, and its connections were still closed")
def _a_channel_that_never_takes_it(scenario: dict) -> None:
    """The platform kills the process on its own schedule, so past some number
    of attempts trying again is time spent not closing anything."""
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a channel that never took it answered {answer!r}, and an operator has "
        f"to be told whose commits are still on the machine"
    )
    assert scenario["databases"][0]["target"] in answer["what_went_wrong"], (
        f"the fault does not name the database whose commits did not leave: "
        f"{answer['what_went_wrong']!r}"
    )
    assert scenario["still_open"] == [], (
        f"{scenario['still_open']} were left open after a push that never got "
        f"through, which is a thread outliving the process on top of lost data"
    )
