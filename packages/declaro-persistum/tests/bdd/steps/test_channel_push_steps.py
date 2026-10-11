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
from typing import Any

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
_WHAT_EACH_GIVEN_SETS: dict[str, dict[str, Any]] = {
    "local commits the primary has not seen": {"writes": True, "how": "a real channel"},
    "a channel with nothing waiting on it": {"writes": False, "how": "a real channel"},
    "a channel the primary will not take": {"writes": False, "how": "a dead primary"},
    "a channel and other work waiting to run": {"writes": False, "how": "a real channel"},
    # MOVED HERE FROM `session_open_channel.feature` ON 2026-10-10, when that door
    # stopped proving the channel with a pull. The push is the first call on a new
    # channel, so it is the call that fills a copy with nothing on it.
    "a primary holding a table this copy has never seen": {
        "writes": False,
        "how": "a copy with nothing on it",
    },
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
                **await _WHAT_THE_SCENARIO_PUSHES[scenario["how"]](scenario),
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
    """A channel to a primary that is not there, opened through the door.

    SINCE 2026-10-10 THE DOOR HANDS THIS BACK, because it stopped proving the
    channel with a pull: the driver builds its sync half on the first push or
    pull, so nothing at open can tell this channel from a live one. This push is
    where a caller finds out, which is what the open relies on.
    """
    channel = await session_open_channel(
        str(scenario["tmp"] / "bad.db"), A_PRIMARY_THAT_DOES_NOT_EXIST, "not-a-token"
    )
    assert not isinstance(channel, dict), (
        f"the door refused a primary that does not exist: {channel}. It reaches "
        f"no primary, so it has nothing to refuse one on, and the open relies on "
        f"this push to find out"
    )
    return {"answer": await channel_push(channel), "written": [], "reached": []}


async def _push_on_a_copy_with_nothing_on_it(scenario: dict) -> dict:
    """A table put on the primary, then a push on a copy that has never pulled.

    WHAT THE PUSH DOES HERE IS BRING THE PRIMARY DOWN, which reads backwards
    until you know the driver builds its sync half on whichever call comes first,
    and `bootstrap_if_empty` fills the copy as it does.
    """
    url, token = _the_staging_primary()
    with tempfile.TemporaryDirectory() as made:
        setting_up = await _a_channel(pathlib.Path(made), "setup.db")
        cursor = setting_up.cursor()
        await cursor.execute(f"CREATE TABLE {scenario['table']} (id integer primary key)")
        await cursor.execute(f"INSERT INTO {scenario['table']} VALUES (1)")
        await setting_up.commit()
        assert await channel_push(setting_up) is None, "the setup push failed"

        cold = await session_open_channel(str(scenario["tmp"] / "cold" / "replica.db"), url, token)
        assert not isinstance(cold, dict), f"the cold channel did not open: {cold}"
        answer = await channel_push(cold)
        cursor = cold.cursor()
        await cursor.execute(f"SELECT id FROM {scenario['table']}")
        held = list(await cursor.fetchall())

        # Take the table off the primary again, through the copy that made it.
        cursor = setting_up.cursor()
        await cursor.execute(f"DROP TABLE {scenario['table']}")
        await setting_up.commit()
        await channel_push(setting_up)
        return {"answer": answer, "written": [], "reached": [], "on_the_cold_copy": held}


_WHAT_THE_SCENARIO_PUSHES = {
    "a real channel": _push_on_a_real_channel,
    "a dead primary": _push_on_an_unreachable_primary,
    "a copy with nothing on it": _push_on_a_copy_with_nothing_on_it,
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


@then("the table is readable on this copy without asking for anything else")
def _the_table_is_readable_here(scenario: dict) -> None:
    """A cold copy that brought nothing down is a database with no schema, which
    `docs/architecture.md:310` calls unusable rather than merely stale."""
    assert scenario["answer"] is None, f"the push came back with {scenario['answer']!r}"
    assert scenario["on_the_cold_copy"] == [(1,)], (
        f"the push left {scenario['table']} holding "
        f"{scenario['on_the_cold_copy']!r} on a copy that had nothing on it, so "
        f"a cold start reads an empty database as its live schema"
    )
