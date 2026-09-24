"""Bind `tests/bdd/features/turso/channel_push.feature`, and nothing else.

WHAT THE FEATURE ASKS. Local commits reach the primary. A push with nothing to
send is quiet. A channel the primary will not take comes back as a fault. And a
push does not stop everything else in the process.

THESE RUN AGAINST THE REAL STAGING PRIMARY. "The primary has them" cannot be
asked of a stand-in without the stand-in deciding the answer, and this is the
one call in the package whose failure loses data silently: measured 2026-09-24,
the driver's push hands back a coroutine and a caller that forgets to await it
gets a clean return in under a millisecond having sent nothing.

HOW "THE PRIMARY HAS THEM" IS READ. Through a second replica opened from the
same primary, which pulls and looks. Asking the same copy that pushed would
answer from the local file and say nothing about what left the machine.

NOTHING IS LEFT ON THE PRIMARY. Each scenario names its table after a fresh
identifier and drops it afterwards, because the staging primary outlives this
process.

WHY THE LAST SCENARIO COUNTS TICKS. "It does not stop everything else" is a
claim about the loop, and the way it fails is silently: a blocking push in an
awaited door looks exactly like a working one from the caller's side. So other
work runs beside it and the scenario counts how much of it got a turn.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import tempfile
import uuid

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.channel_pull import channel_pull
from declaro_persistum.channel_push import channel_push
from declaro_persistum.session_open_channel import session_open_channel

scenarios("../features/turso/channel_push.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

A_PRIMARY_THAT_DOES_NOT_EXIST = "https://nope.invalid"

# How often the other work takes a turn while a push is in flight, and how many
# turns it must get. A push takes about 1.6 seconds against the staging primary,
# so a loop that is free gives out well over a hundred; one turn is enough to
# say the loop was not held, and this asks for ten.
HOW_OFTEN_THE_OTHER_WORK_RUNS = 0.01
HOW_MANY_TURNS_IT_MUST_GET = 10


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path}


def _the_staging_primary() -> tuple[str, str]:
    url = os.environ.get("TEST_TURSO_URL", "")
    token = os.environ.get("TEST_TURSO_AUTH_TOKEN", "")
    missing = [
        name
        for name, value in (("TEST_TURSO_URL", url), ("TEST_TURSO_AUTH_TOKEN", token))
        if not value
    ]
    assert not missing, (
        f"{', '.join(missing)} is not set in .env, and these scenarios run "
        f"against the real primary. A skip would read as a tested backend."
    )
    return url, token


async def _a_channel(folder: pathlib.Path, name: str):
    """One channel of this copy's own, opened through the door that opens them."""
    url, token = _the_staging_primary()
    channel = await session_open_channel(str(folder / name), url, token)
    assert not isinstance(channel, dict), f"the channel did not open: {channel}"
    return channel


async def _other_work(turns: list[int]) -> None:
    """Something else on the loop, taking a turn as often as it can."""
    while True:
        turns.append(len(turns))
        await asyncio.sleep(HOW_OFTEN_THE_OTHER_WORK_RUNS)


# --- what each scenario establishes ------------------------------------------


# WHAT EACH SENTENCE SETS UP: whether anything is written before the push, and
# whether the primary is one that exists. Four sentences, one function over a
# table, because four one-line steps are one step differing by a word.
_WHAT_EACH_GIVEN_SETS: dict[str, dict[str, bool]] = {
    "local commits the primary has not seen": {"writes": True, "unreachable": False},
    "a channel with nothing waiting on it": {"writes": False, "unreachable": False},
    "a channel the primary will not take": {"writes": False, "unreachable": True},
    "a channel and other work waiting to run": {"writes": False, "unreachable": False},
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(_WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_set_up(scenario: dict, sentence: str) -> None:
    scenario.update(_WHAT_EACH_GIVEN_SETS[sentence])
    scenario["table"] = "push_" + uuid.uuid4().hex[:10]


@when("a push runs")
def _a_push_runs(scenario: dict) -> None:
    async def _pushed() -> dict:
        turns: list[int] = []
        beside = asyncio.ensure_future(_other_work(turns))
        try:
            return {
                **await _WHAT_THE_SCENARIO_PUSHES[scenario["unreachable"]](scenario),
                "turns": len(turns),
            }
        finally:
            beside.cancel()

    scenario.update(asyncio.run(_pushed()))


async def _push_on_a_real_channel(scenario: dict) -> dict:
    """Open a channel, optionally write to it, push, and see what the primary got."""
    channel = await _a_channel(scenario["tmp"], "a.db")
    written = await _WHETHER_IT_WRITES[scenario["writes"]](channel, scenario["table"])
    answer = await channel_push(channel)
    return {
        "answer": answer,
        "written": written,
        "reached": await _WHAT_THE_PRIMARY_HAS[scenario["writes"]](scenario),
    }


async def _push_on_an_unreachable_primary(scenario: dict) -> dict:
    """A channel the driver hands back for a primary that is not there.

    IT IS MADE BY THE DRIVER RATHER THAN BY THE DOOR, because the door refuses
    this primary now, and the feature asks what a push does when it is handed
    one anyway. That is the shape a caller holds if any other path ever hands
    one over.
    """
    import turso.aio.sync

    channel = turso.aio.sync.connect(
        str(scenario["tmp"] / "bad.db"), A_PRIMARY_THAT_DOES_NOT_EXIST, auth_token="x"
    )
    return {"answer": await channel_push(channel), "written": [], "reached": []}


_WHAT_THE_SCENARIO_PUSHES = {
    False: _push_on_a_real_channel,
    True: _push_on_an_unreachable_primary,
}


# BOTH HALVES HAND BACK WHAT WAS WRITTEN, so neither is a call made for its
# effect and the assertion below compares the primary against the rows this
# scenario actually put in rather than against a literal somebody typed.
async def _write_something(channel, table: str) -> list:
    cursor = channel.cursor()
    await cursor.execute(f"CREATE TABLE {table} (id integer primary key, note text)")
    await cursor.execute(f"INSERT INTO {table} VALUES (1, 'sent')")
    await channel.commit()
    return [("sent",)]


async def _write_nothing(_channel, _table: str) -> list:
    return []


_WHETHER_IT_WRITES = {True: _write_something, False: _write_nothing}


async def _ask_a_second_replica(scenario: dict) -> list:
    """What the primary has, read through a copy that never saw the write.

    ASKING THE COPY THAT PUSHED WOULD ANSWER FROM THE LOCAL FILE and say nothing
    about what left the machine, which is the whole of what a push is for.
    """
    with tempfile.TemporaryDirectory() as made:
        other = await _a_channel(pathlib.Path(made), "b.db")
        assert await channel_pull(other) is None, "the second replica could not pull"
        cursor = other.cursor()
        await cursor.execute(f"SELECT note FROM {scenario['table']}")
        held = list(await cursor.fetchall())
        await cursor.execute(f"DROP TABLE {scenario['table']}")
        await other.commit()
        await channel_push(other)
        return held


async def _ask_nothing(_scenario: dict) -> list:
    """Nothing was written, so nothing is asked and the answer is the empty list.

    THE SCENARIO THAT READS THIS IS THE ONE THAT WROTE, so an empty list here is
    never mistaken for a primary that had nothing: the two are told apart by
    which scenario is running, and both halves answer in the same shape.
    """
    return []


_WHAT_THE_PRIMARY_HAS = {True: _ask_a_second_replica, False: _ask_nothing}


# --- what must be true afterwards --------------------------------------------


@then("the primary has them")
def _the_primary_has_them(scenario: dict) -> None:
    assert scenario["answer"] is None, f"the push came back with {scenario['answer']!r}"
    assert scenario["reached"] == scenario["written"], (
        f"this copy wrote {scenario['written']!r} and a second replica pulling "
        f"from the primary sees {scenario['reached']!r}, so the commits never "
        f"left this machine"
    )


@then("nothing comes back, and it is not a fault")
def _nothing_comes_back(scenario: dict) -> None:
    assert scenario["answer"] is None, (
        f"a push with nothing to send answered {scenario['answer']!r}, which makes "
        f"an idle database report a failure on every free turn"
    )


@then("a fault comes back saying what to do about it")
def _a_fault_comes_back(scenario: dict) -> None:
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a push the primary would not take answered {answer!r}, and local "
        f"commits that did not leave are what shutdown has to hear about"
    )
    assert answer["what_to_do_instead"].strip() != "", (
        "the fault says nothing about what to do, and the caller cannot retry "
        "what it cannot understand"
    )


@then(parsers.re(r"^the other work ran while the (?:push|pull) was in flight$"))
def _the_other_work_ran(scenario: dict) -> None:
    """A blocking call in an awaited door looks exactly like a working one from
    the caller's side, and holds every other request in the process."""
    assert scenario["turns"] >= HOW_MANY_TURNS_IT_MUST_GET, (
        f"other work got {scenario['turns']} turns while the call was in flight, "
        f"and it asks for {HOW_MANY_TURNS_IT_MUST_GET}. The loop was held, so "
        f"every other request in the process waited for a sync it has nothing "
        f"to do with"
    )
