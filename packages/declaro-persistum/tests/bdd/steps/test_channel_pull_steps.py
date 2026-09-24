"""Bind `tests/bdd/features/turso/channel_pull.feature`, and nothing else.

WHAT THE FEATURE ASKS. What the primary has and this copy does not comes down. A
pull with nothing to bring is quiet. A channel the primary will not take comes
back as a fault. And a pull does not stop everything else in the process.

THESE RUN AGAINST THE REAL STAGING PRIMARY. "This copy has them" cannot be asked
of a stand-in without the stand-in deciding the answer, and a pull that silently
brings nothing down is a database answering every read from a copy that never
heard from the primary: measured 2026-09-24, the driver's pull hands back a
coroutine and a caller that forgets to await it gets a clean return having
fetched nothing.

WHAT THE PRIMARY HOLDS IS PUT THERE THROUGH THIS PACKAGE'S OWN PUSH, so a
scenario that cannot be set up is a broken push rather than a mystery, and the
two halves of the channel are exercised against each other.

THE COPY THAT PULLS IS OPENED BEFORE THE ROWS EXIST. Opening it afterwards would
let the cold bootstrap bring them down and the pull would be asserting nothing.

NOTHING IS LEFT ON THE PRIMARY. Each scenario names its table after a fresh
identifier and drops it afterwards.
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

scenarios("../features/turso/channel_pull.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

A_PRIMARY_THAT_DOES_NOT_EXIST = "https://nope.invalid"

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
    url, token = _the_staging_primary()
    channel = await session_open_channel(str(folder / name), url, token)
    assert not isinstance(channel, dict), f"the channel did not open: {channel}"
    return channel


async def _other_work(turns: list[int]) -> None:
    while True:
        turns.append(len(turns))
        await asyncio.sleep(HOW_OFTEN_THE_OTHER_WORK_RUNS)


# --- what each scenario establishes ------------------------------------------


# WHAT EACH SENTENCE SETS UP: whether the primary gains rows this copy has not
# seen, and whether it is a primary that exists. Four sentences, one function
# over a table.
_WHAT_EACH_GIVEN_SETS: dict[str, dict[str, bool]] = {
    "a primary holding rows this copy has never seen": {
        "brings_something": True,
        "unreachable": False,
    },
    "a copy already level with the primary": {"brings_something": False, "unreachable": False},
    "a channel the primary will not take": {"brings_something": False, "unreachable": True},
    "a channel and other work waiting to run": {
        "brings_something": False,
        "unreachable": False,
    },
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(_WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_set_up(scenario: dict, sentence: str) -> None:
    scenario.update(_WHAT_EACH_GIVEN_SETS[sentence])
    scenario["table"] = "pull_" + uuid.uuid4().hex[:10]


@when("a pull runs")
def _a_pull_runs(scenario: dict) -> None:
    async def _pulled() -> dict:
        turns: list[int] = []
        beside = asyncio.ensure_future(_other_work(turns))
        try:
            return {
                **await _WHAT_THE_SCENARIO_PULLS[scenario["unreachable"]](scenario),
                "turns": len(turns),
            }
        finally:
            beside.cancel()

    scenario.update(asyncio.run(_pulled()))


async def _pull_on_a_real_channel(scenario: dict) -> dict:
    """Open the copy first, then put rows on the primary, then pull.

    THE ORDER IS THE WHOLE SCENARIO. A copy opened after the rows exist gets them
    from the cold bootstrap, and the pull would be asserting nothing.
    """
    mine = await _a_channel(scenario["tmp"], "mine.db")
    went_up = await _WHETHER_THE_PRIMARY_GAINS_SOMETHING[scenario["brings_something"]](
        scenario["table"]
    )
    answer = await channel_pull(mine)
    return {
        "answer": answer,
        "went_up": went_up,
        "seen": await _WHAT_THIS_COPY_SEES[scenario["brings_something"]](mine, scenario["table"]),
    }


async def _pull_on_an_unreachable_primary(scenario: dict) -> dict:
    """A channel the driver hands back for a primary that is not there."""
    import turso.aio.sync

    channel = turso.aio.sync.connect(
        str(scenario["tmp"] / "bad.db"), A_PRIMARY_THAT_DOES_NOT_EXIST, auth_token="x"
    )
    return {"answer": await channel_pull(channel), "went_up": [], "seen": []}


_WHAT_THE_SCENARIO_PULLS = {
    False: _pull_on_a_real_channel,
    True: _pull_on_an_unreachable_primary,
}


# BOTH HALVES HAND BACK WHAT THE PRIMARY GAINED, so neither is a call made for
# its effect and the assertion below compares this copy against the rows that
# actually went up rather than against a literal somebody typed.
async def _put_rows_on_the_primary(table: str) -> list:
    """Through this package's own push, so a broken push fails the setup here."""
    with tempfile.TemporaryDirectory() as made:
        other = await _a_channel(pathlib.Path(made), "other.db")
        cursor = other.cursor()
        await cursor.execute(f"CREATE TABLE {table} (id integer primary key, note text)")
        await cursor.execute(f"INSERT INTO {table} VALUES (1, 'brought down')")
        await other.commit()
        assert await channel_push(other) is None, "the setup push failed"
    return [("brought down",)]


async def _change_nothing(_table: str) -> list:
    """The primary gains nothing, which is a real answer in the same shape."""
    return []


_WHETHER_THE_PRIMARY_GAINS_SOMETHING = {True: _put_rows_on_the_primary, False: _change_nothing}


async def _read_the_table(channel, table: str) -> list:
    cursor = channel.cursor()
    await cursor.execute(f"SELECT note FROM {table}")
    held = list(await cursor.fetchall())
    await cursor.execute(f"DROP TABLE {table}")
    await channel.commit()
    await channel_push(channel)
    return held


async def _read_nothing(_channel, _table: str) -> list:
    """Nothing was put up, so nothing is read back, in the same shape."""
    return []


_WHAT_THIS_COPY_SEES = {True: _read_the_table, False: _read_nothing}


# --- what must be true afterwards --------------------------------------------


@then("this copy has them")
def _this_copy_has_them(scenario: dict) -> None:
    assert scenario["answer"] is None, f"the pull came back with {scenario['answer']!r}"
    assert scenario["seen"] == scenario["went_up"], (
        f"{scenario['went_up']!r} went up to the primary and this copy sees "
        f"{scenario['seen']!r} after a pull, so nothing came down and every read "
        f"here answers from a copy that never heard from the primary"
    )


@then("nothing comes back, and it is not a fault")
def _nothing_comes_back(scenario: dict) -> None:
    assert scenario["answer"] is None, (
        f"a pull with nothing to bring down answered {scenario['answer']!r}, and "
        f"a copy already level with the primary is the good case"
    )


@then("a fault comes back saying what to do about it")
def _a_fault_comes_back(scenario: dict) -> None:
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a pull the primary would not answer came back as {answer!r}, and a "
        f"caller reading stale rows believing them current is the failure this "
        f"prevents"
    )
    assert answer["what_to_do_instead"].strip() != "", "the fault says nothing about what to do"


@then(parsers.re(r"^the other work ran while the (?:push|pull) was in flight$"))
def _the_other_work_ran(scenario: dict) -> None:
    assert scenario["turns"] >= HOW_MANY_TURNS_IT_MUST_GET, (
        f"other work got {scenario['turns']} turns while the pull was in flight, "
        f"and it asks for {HOW_MANY_TURNS_IT_MUST_GET}. The loop was held"
    )
