"""Bind `tests/bdd/features/session/session_open_channel.feature`.

WHAT THE FEATURE ASKS. A copy is made where the path says, with the folder if
there is not one. A cold open brings the primary's schema down. A primary that
cannot be reached is a fault rather than a handle.

THESE RUN AGAINST THE REAL STAGING PRIMARY named by TEST_TURSO_URL, and the
scenarios that do carry `@network`. A stand-in here would be standing in for the
one thing that matters: `docs/design/provisioning` records a scenario that
replaced the driver with a double, asserted the double had been asked for the
path, and passed, while the real driver refused that exact path shape on the
first run against it.

NOTHING IS LEFT ON THE PRIMARY. Each scenario that writes makes a table named
after a fresh identifier and drops it afterwards, because the staging primary
outlives this process and a table left there is a table the next run meets.

WHY "A COPY IS THERE" IS READ OFF THE DISK. A channel that came back is not
evidence of a file: the driver hands one back for a primary that does not exist.
So the path is looked at.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import tempfile
import uuid

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.channel_push import channel_push
from declaro_persistum.session_open_channel import session_open_channel

scenarios("../features/session/session_open_channel.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

A_PRIMARY_THAT_DOES_NOT_EXIST = "https://nope.invalid"
A_TOKEN_IT_WOULD_NOT_TAKE = "not-a-token"


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path}


def _the_staging_primary() -> tuple[str, str]:
    """The live primary this suite replicates from, or a failure naming what is
    missing. A skip here would read as a tested backend."""
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


# --- what each scenario establishes ------------------------------------------


@given("a path in a folder that exists, with no copy at it yet")
def _a_path_in_a_folder_that_exists(scenario: dict) -> None:
    scenario["path"] = str(scenario["tmp"] / "replica.db")
    assert not pathlib.Path(scenario["path"]).exists()


@given("a path whose folder does not exist")
def _a_path_whose_folder_does_not_exist(scenario: dict) -> None:
    scenario["folder"] = scenario["tmp"] / "not" / "made" / "yet"
    scenario["path"] = str(scenario["folder"] / "replica.db")
    assert not scenario["folder"].exists()


@given("a primary holding a table this path has never seen")
def _a_primary_holding_a_table(scenario: dict) -> None:
    """A table put on the primary through a channel of its own, then pushed.

    IT GOES UP THROUGH THE DOOR UNDER TEST'S OWN SIBLING, `fn channel_push`, so
    the setup uses the package rather than the driver and a broken push shows up
    as a scenario that cannot be set up.
    """
    url, token = _the_staging_primary()
    table = "cold_" + uuid.uuid4().hex[:10]

    async def _put_it_there() -> None:
        with tempfile.TemporaryDirectory() as made:
            channel = await session_open_channel(str(pathlib.Path(made) / "a.db"), url, token)
            assert not isinstance(channel, dict), f"the setup channel failed: {channel}"
            cursor = channel.cursor()
            await cursor.execute(f"CREATE TABLE {table} (id integer primary key)")
            await cursor.execute(f"INSERT INTO {table} VALUES (1)")
            await channel.commit()
            assert await channel_push(channel) is None, "the setup push failed"

    asyncio.run(_put_it_there())
    scenario["table"] = table
    scenario["path"] = str(scenario["tmp"] / "cold" / "replica.db")


@given("a primary that does not exist")
def _a_primary_that_does_not_exist(scenario: dict) -> None:
    scenario["path"] = str(scenario["tmp"] / "replica.db")
    scenario["primary"] = A_PRIMARY_THAT_DOES_NOT_EXIST
    scenario["token"] = A_TOKEN_IT_WOULD_NOT_TAKE


@when(parsers.re(r"^a channel is opened (?:at that path|to it)$"))
def _a_channel_is_opened(scenario: dict) -> None:
    primary = scenario.get("primary")
    token = scenario.get("token")
    url, real_token = {
        True: lambda: (primary, token),
        False: _the_staging_primary,
    }[primary is not None]()

    async def _opened() -> dict:
        channel = await session_open_channel(scenario["path"], url, real_token)  # type: ignore[arg-type]
        return {
            "answer": channel,
            "rows": await _READ_BACK[scenario.get("table") is not None](
                channel, scenario.get("table", "")
            ),
        }

    scenario.update(asyncio.run(_opened()))


# A CHANNEL BELONGS TO THE LOOP THAT MADE IT, so a scenario that reads one has to
# read it inside the same `asyncio.run` that opened it. Both rows are here, and
# the row that reads nothing answers with an empty list rather than doing
# nothing, so the pair produces the value the `Then` reads.


async def _nothing_to_read_back(_channel: object, _table: str) -> list:
    return []


async def _read_the_table(channel: object, table: str) -> list:
    """Read the table the primary had, then take it off the primary again."""
    cursor = channel.cursor()  # type: ignore[attr-defined]
    await cursor.execute(f"SELECT id FROM {table}")
    held = await cursor.fetchall()
    await cursor.execute(f"DROP TABLE {table}")
    await channel.commit()  # type: ignore[attr-defined]
    await channel_push(channel)  # type: ignore[arg-type]
    return list(held)


_READ_BACK = {False: _nothing_to_read_back, True: _read_the_table}


# --- what must be true afterwards --------------------------------------------


@then("a copy is there")
def _a_copy_is_there(scenario: dict) -> None:
    """Read off the disk, because a channel coming back is not evidence of one."""
    assert pathlib.Path(scenario["path"]).exists(), (
        f"the channel came back and there is no copy at {scenario['path']}, so "
        f"nothing was replicated into"
    )


@then("the folder was made")
def _the_folder_was_made(scenario: dict) -> None:
    """The driver takes a path and will not make the directory around it, and a
    fresh machine has neither."""
    assert scenario["folder"].exists(), (
        f"{scenario['folder']} was not made, so a deployment on a fresh disk is "
        f"refused for a folder it asked this library to put a database in"
    )


@then("the table is readable without asking for anything else")
def _the_table_is_readable(scenario: dict) -> None:
    """A cold open that brought nothing down is a database with no schema, which
    `docs/architecture.md:310` calls unusable rather than merely stale."""
    assert not isinstance(scenario["answer"], dict), (
        f"the channel did not open: {scenario['answer']}"
    )
    assert scenario["rows"] == [(1,)], (
        f"the cold open left {scenario['table']} holding {scenario['rows']!r}, "
        f"so the copy came back without the primary's schema on it"
    )


@then("a fault comes back naming the primary")
def _a_fault_names_the_primary(scenario: dict) -> None:
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a primary that does not exist answered {answer!r}. The driver hands "
        f"back a connection for one, and every push on it raises about a "
        f"NoneType, so a handle here is a database that loses everything at "
        f"shutdown"
    )
    assert A_PRIMARY_THAT_DOES_NOT_EXIST in answer["what_went_wrong"], (
        f"the fault does not name the primary that could not be reached: "
        f"{answer['what_went_wrong']!r}"
    )


@then("no channel is handed back")
def _no_channel_is_handed_back(scenario: dict) -> None:
    assert not hasattr(scenario["answer"], "cursor"), (
        "a connection came back beside the fault, and a caller holding one "
        "writes to it believing it replicates"
    )
