"""Bind `tests/bdd/features/turso/turso_pull.feature`.

WHAT THE FEATURE ASKS FOR. Five scenarios, all of them about one direction of
the sync channel: what the primary has and this copy does not. Two came from
the opening story, three were folded in from `provisioning_a_copy.feature` on
2026-09-21, and all five run against the real Turso primary named by
`TEST_TURSO_URL`.

NO STAND-IN FOR THE DRIVER, AND THE REASON IS WRITTEN IN THE FEATURE. A double
that accepted any path hid the fact that pyturso will not make the folder a
database file sits in, and the scenario aimed at exactly that case had been
green for months. So every connection here is a real one against the real
primary, and a machine that cannot reach it says so rather than skipping.

WHAT THE HARNESS DOES THAT `fn turso_pull` DOES NOT. The card takes a
`Database`, which already holds its connection, so nothing in it opens a file
or makes a folder. Opening is `fn session_open_connection`, which is not built
yet, so `_opened` below stands in for it: it makes the folder and asks
pyturso for a replica. That is the only part of these scenarios the function
under test does not own, and it is why the two folder scenarios pass today —
see the report beside this branch.

WHAT MAKES THE ASSERTIONS BITE. pyturso bootstraps a brand-new replica during
`connect`, so a copy that has never existed arrives full whether or not anybody
pulls. Every scenario here therefore moves the primary AFTER the local copy
exists, or asks a copy that already exists for what it is missing. Delete the
pull and `a cold open copies the whole database` goes red.

WHY ONE EVENT LOOP PER SCENARIO. pyturso's replicated connection is opened,
pulled and closed by three different steps. Three calls to `asyncio.run` would
be three loops and one connection, so the loop is a fixture that closes what it
made, which is the shape `test_nothing_takes_an_event_loop_it_does_not_own.py`
permits.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import tempfile
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.room_make import room_make

turso = pytest.importorskip(
    "turso", reason="pyturso is not installed, so there is no primary to pull from"
)
turso_sync = pytest.importorskip("turso.aio.sync")

scenarios("../features/turso/turso_pull.feature")


# How long the pull on a warm copy may take before it counts as waiting for
# Turso. `docs/architecture.md` puts no consumer request path behind the
# primary, and a warm open is a consumer request path.
A_SHORT_WAIT_SECONDS = 5.0

# Batches the harness pulls when it reads the primary for itself. It reads the
# primary through a replica of its own, and using the function under test to
# measure the function under test would let one bug hide another.
A_HARNESS_BUDGET = 64


# ---------------------------------------------------------------------------
# What a scenario needs before it can ask anything
# ---------------------------------------------------------------------------


@pytest.fixture
def the_primary() -> str:
    """The live Turso primary, or a skip that names what is not set."""
    return os.environ.get("TEST_TURSO_URL") or pytest.skip(
        "TEST_TURSO_URL names the Turso primary these scenarios pull from, and "
        "it is unset. Set it and TEST_TURSO_AUTH_TOKEN, or run with "
        '-m "not network".'
    )


@pytest.fixture
def the_token() -> str | None:
    """The credential for that primary. Absent is a real case, not a default."""
    return os.environ.get("TEST_TURSO_AUTH_TOKEN")


@pytest.fixture
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    """One loop per scenario, closed by whoever opened it."""
    made = asyncio.new_event_loop()
    try:
        yield made
    finally:
        made.close()


@pytest.fixture
def workspace() -> Iterator[pathlib.Path]:
    """A folder of this scenario's own, removed whole.

    A REPLICA IS FOUR FILES AND ONE ATOMIC UNIT: `x.db`, `x.db-changes`,
    `x.db-info` and `x.db-wal`. Removing a subset breaks the next open, so the
    folder goes rather than the file.
    """
    made = pathlib.Path(tempfile.mkdtemp(prefix="turso-pull-"))
    try:
        yield made
    finally:
        shutil.rmtree(made, ignore_errors=True)


@pytest.fixture
def the_witness(
    loop: asyncio.AbstractEventLoop,
    workspace: pathlib.Path,
    the_primary: str,
    the_token: str | None,
) -> Iterator[Any]:
    """A replica of the harness's own, used to write the primary and read it back.

    It is a second replica rather than a second handle on the first, because
    the scenario has to be able to move the primary while the copy under test
    sits still. That is the only way a pull has anything to fetch.
    """
    try:
        held = loop.run_until_complete(
            _opened(str(workspace / "witness" / "witness.db"), the_primary, the_token)
        )
    except turso.Error as unreachable:
        pytest.fail(
            f"the Turso primary at {the_primary} is not reachable: "
            f"{type(unreachable).__name__}: {unreachable}. These scenarios pull "
            f"from it, so this is a failure and not a skip: a skipped backend "
            f"reads as a tested one."
        )
    try:
        yield held
    finally:
        loop.run_until_complete(held.close())


@pytest.fixture
def our_table(loop: asyncio.AbstractEventLoop, the_witness: Any) -> Iterator[str]:
    """A table name nothing else on this shared staging primary uses.

    Dropped at the end whether or not it was ever created, because a scenario
    that fails half way through still leaves the primary as it found it.
    """
    named = "pull_" + uuid.uuid4().hex[:12]
    try:
        yield named
    finally:
        loop.run_until_complete(_told(the_witness, [f"DROP TABLE IF EXISTS {named}"]))


# ---------------------------------------------------------------------------
# Talking to the driver, in the few shapes these scenarios need
# ---------------------------------------------------------------------------


async def _opened(path: str, primary: str, token: str | None) -> Any:
    """Make the folder and open a replica at `path`.

    THE FOLDER IS MADE HERE BECAUSE NOBODY ELSE MAKES IT. pyturso refuses a
    path whose folder is absent with `I/O error (open): entity not found`,
    which names neither. `fn session_open_connection` is where this belongs
    once it exists; `fn turso_pull` is handed a connection and never a path.
    """
    pathlib.Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
    return await turso_sync.connect(path, remote_url=primary, auth_token=token)


async def _rows(connection: Any, text: str) -> list[tuple]:
    """Every row one statement answers with."""
    cursor = await connection.execute(text)
    return list(await cursor.fetchall())


async def _told(connection: Any, statements: list[str]) -> None:
    """Run these statements and send them to the primary."""
    for text in statements:
        await connection.execute(text)
    await connection.commit()
    await connection.push()


async def _caught_up(connection: Any) -> None:
    """Pull until the primary reports nothing further, the harness's own way."""
    remaining = A_HARNESS_BUDGET
    fetched = True
    while fetched and remaining > 0:
        fetched = await connection.pull()
        remaining -= 1


async def _what_the_primary_holds(connection: Any, table: str) -> tuple[bool, list[tuple]]:
    """Our table's presence and contents, as the primary has them now.

    ONLY OUR TABLE, on purpose. The staging primary is shared, and other work
    on this machine writes to it while these scenarios run, so a snapshot of
    every table would measure them rather than us.
    """
    await _caught_up(connection)
    named = await _rows(connection, f"SELECT name FROM sqlite_schema WHERE name = '{table}'")
    return (named != [], await _rows_of(connection, table))


async def _rows_of(connection: Any, table: str) -> list[tuple]:
    """The table's rows, or none when the table is not there to have any."""
    try:
        return sorted(await _rows(connection, f"SELECT v FROM {table}"))
    except turso.Error:
        return []


def _a_database_over(connection: Any, primary: str) -> dict:
    """The `Database` the card is handed: a handle, a primary, and a shutdown
    policy that says this one replicates on the way out."""
    return {
        "engine": "turso",
        "connection": connection,
        "writers": [],
        "room": room_make(),
        "shutdown": "replicate_then_exit",
        "primary": primary,
    }


# ---------------------------------------------------------------------------
# Given: four starting points, one step
# ---------------------------------------------------------------------------


def _a_copy_with_no_schema(state: dict, workspace, primary, token, table, witness) -> None:
    """A copy that exists and knows nothing about the table the primary gains.

    This is the cold open. pyturso fills a brand-new replica during `connect`,
    so the only honest way to have a copy with no schema is to open it before
    the schema exists and move the primary afterwards.
    """
    state["path"] = str(workspace / "cold" / "copy.db")
    connection = state["loop"].run_until_complete(_opened(state["path"], primary, token))
    state["db"] = _a_database_over(connection, primary)
    state["before"] = state["loop"].run_until_complete(
        _rows(connection, f"SELECT name FROM sqlite_schema WHERE name = '{table}'")
    )
    state["loop"].run_until_complete(
        _told(
            witness,
            [
                f"CREATE TABLE {table} (v TEXT)",
                f"INSERT INTO {table} VALUES ('one')",
                f"INSERT INTO {table} VALUES ('two')",
            ],
        )
    )


def _a_copy_with_the_schema(state: dict, workspace, primary, token, table, witness) -> None:
    """A warm copy: the schema is already on its disk, and its channel is shut.

    A WARM OPEN MUST NOT WAIT FOR TURSO, so this is the copy that has to stay
    usable when the primary cannot be reached. Closing the channel is how a
    test produces that without unplugging the machine: pyturso refuses a pull
    on a closed channel with a `turso.Error`, the same class it raises when the
    primary itself refuses one (measured 2026-09-22, a bad credential gives
    `turso.DatabaseError`), so one path in the card answers both.
    """
    state["loop"].run_until_complete(
        _told(witness, [f"CREATE TABLE {table} (v TEXT)", f"INSERT INTO {table} VALUES ('one')"])
    )
    state["path"] = str(workspace / "warm" / "copy.db")
    connection = state["loop"].run_until_complete(_opened(state["path"], primary, token))
    state["before"] = state["loop"].run_until_complete(_rows(connection, f"SELECT v FROM {table}"))
    state["db"] = _a_database_over(connection, primary)
    state["loop"].run_until_complete(connection.close())


def _a_path_in_a_folder(state: dict, workspace, primary, token, table, witness) -> None:
    """A path whose folder is there and whose copy is not."""
    state["loop"].run_until_complete(
        _told(witness, [f"CREATE TABLE {table} (v TEXT)", f"INSERT INTO {table} VALUES ('one')"])
    )
    state["path"] = str(workspace / "copy.db")
    state["before"] = sorted(p.name for p in workspace.iterdir())
    state["primary_before"] = state["loop"].run_until_complete(
        _what_the_primary_holds(witness, table)
    )


def _a_path_with_no_folder(state: dict, workspace, primary, token, table, witness) -> None:
    """A path two folders deep, neither of which exists. A service on a fresh
    disk names the path it wants and the folder is not there yet."""
    state["loop"].run_until_complete(
        _told(witness, [f"CREATE TABLE {table} (v TEXT)", f"INSERT INTO {table} VALUES ('one')"])
    )
    state["path"] = str(workspace / "tenants" / "acme" / "copy.db")
    state["before"] = sorted(p.name for p in workspace.iterdir())
    state["primary_before"] = state["loop"].run_until_complete(
        _what_the_primary_holds(witness, table)
    )


# What the feature's four openings mean, as the function that builds each one.
# Four step functions differing only in which sentence they carry would be one
# function written four times, which is the shape this package deletes.
A_STARTING_POINT = {
    "a local copy with no schema": _a_copy_with_no_schema,
    "a local copy that already has the schema": _a_copy_with_the_schema,
    "a path in a folder that exists, with no copy at it yet": _a_path_in_a_folder,
    "a path whose folder does not exist": _a_path_with_no_folder,
}


@given(
    parsers.re(
        r"(?P<starting_point>a local copy with no schema"
        r"|a local copy that already has the schema"
        r"|a path in a folder that exists, with no copy at it yet"
        r"|a path whose folder does not exist)$"
    )
)
def _a_starting_point(
    scenario: dict,
    loop: asyncio.AbstractEventLoop,
    workspace: pathlib.Path,
    the_primary: str,
    the_token: str | None,
    our_table: str,
    the_witness: Any,
    starting_point: str,
) -> None:
    scenario["loop"] = loop
    scenario["table"] = our_table
    A_STARTING_POINT[starting_point](
        scenario, workspace, the_primary, the_token, our_table, the_witness
    )


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the database is opened")
def _the_database_is_opened(scenario: dict) -> None:
    """The moment the orchestrator reaches the card, timed."""
    from declaro_persistum.turso_pull import turso_pull

    started = time.monotonic()
    scenario["fault"] = scenario["loop"].run_until_complete(turso_pull(scenario["db"]))
    scenario["waited"] = time.monotonic() - started


@when(parsers.re(r"a (?P<asking>copy is asked for|replica is opened) at that path$"))
def _a_copy_is_asked_for(scenario: dict, the_primary: str, the_token: str | None) -> None:
    """Open a replica where the path says, then bring down what it lacks.

    One step for both sentences, because asking for a copy at a path and
    opening a replica at one are the same request in different words.
    """
    from declaro_persistum.turso_pull import turso_pull

    async def asking() -> tuple[dict, object]:
        connection = await _opened(scenario["path"], the_primary, the_token)
        db = _a_database_over(connection, the_primary)
        return db, await turso_pull(db)

    scenario["db"], scenario["fault"] = scenario["loop"].run_until_complete(asking())


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then("the whole database was copied before the handle came back")
def _the_whole_database_was_copied(scenario: dict) -> None:
    """Everything the primary gained is readable the moment the pull returned.

    Nothing pulls between the `When` and this read, so a copy that is missing a
    row here is a copy that was still filling when the caller got it back.
    """
    assert scenario["before"] == [], (
        f"the copy already knew {scenario['table']} before the pull, so this "
        f"scenario cannot tell a working pull from a no-op"
    )
    assert scenario["fault"] is None, f"the pull came back with a fault: {scenario['fault']}"

    arrived = scenario["loop"].run_until_complete(
        _rows(scenario["db"]["connection"], f"SELECT v FROM {scenario['table']} ORDER BY v")
    )
    assert arrived == [("one",), ("two",)], (
        f"the copy holds {arrived} and the primary holds two rows, so the pull "
        f"returned before the whole database was down"
    )


@then("the handle came back without waiting for Turso")
def _the_handle_came_back(scenario: dict) -> None:
    """A pull that cannot reach the primary is a value, and the copy still reads.

    Three things, all of them the standing requirement: it came back rather
    than raising, it came back quickly, and the database is still usable from
    its local copy afterwards.
    """
    assert scenario["before"] == [("one",)], (
        "the warm copy did not have the schema before the pull, so nothing "
        "here says a warm open can answer without Turso"
    )
    fault = scenario["fault"]
    assert isinstance(fault, dict), f"an unreachable primary produced {fault!r}, not a Fault"
    assert fault["kind"] == "cut_short", fault
    assert fault["what_went_wrong"] and fault["what_to_do_instead"], (
        f"a fault that says neither what happened nor what to do is a log line "
        f"wearing a return value: {fault}"
    )
    assert scenario["waited"] < A_SHORT_WAIT_SECONDS, (
        f"the pull held the caller for {scenario['waited']:.1f}s, and a warm "
        f"open must not wait on the primary at all"
    )

    # NOTHING IN pyturso IS NATIVELY SYNCHRONOUS. `turso.aio` is the driver and
    # `turso.connect` is a shim over it, so the read here is awaited like every
    # other. `turso.aio.connect` also asks for the running loop while it builds,
    # so it is called inside the coroutine rather than handed to one.
    import turso.aio

    async def _read_the_copy() -> list:
        local = await turso.aio.connect(scenario["path"])
        try:
            cursor = await local.execute(f"SELECT v FROM {scenario['table']}")
            return list(await cursor.fetchall())
        finally:
            await local.close()

    still_there = asyncio.new_event_loop().run_until_complete(_read_the_copy())
    assert still_there == [("one",)], (
        f"the local copy answers {still_there} after the failed pull, so the "
        f"database did not stay usable from it"
    )


@then("a copy is there")
def _a_copy_is_there(scenario: dict) -> None:
    """The four files exist at the path asked for, and hold the primary's rows."""
    assert scenario["fault"] is None, f"asking for a copy came back with: {scenario['fault']}"

    at_the_path = pathlib.Path(scenario["path"])
    assert at_the_path.exists(), f"nothing was made at {at_the_path}"

    held = scenario["loop"].run_until_complete(
        _rows(scenario["db"]["connection"], f"SELECT v FROM {scenario['table']}")
    )
    assert held == [("one",)], (
        f"the copy at {at_the_path} holds {held}, so it is a file rather than a copy of the primary"
    )


@then("the primary is unchanged")
def _the_primary_is_unchanged(scenario: dict, the_witness: Any) -> None:
    """A reader reads. It does not push, and it does not write bookkeeping back.

    Read through the harness's own replica, which is pulled fresh first, so
    what is compared is the primary and not a stale local view of it.
    """
    after = scenario["loop"].run_until_complete(
        _what_the_primary_holds(the_witness, scenario["table"])
    )
    assert after == scenario["primary_before"], (
        f"the primary held {scenario['primary_before']} before the pull and "
        f"{after} after it, so a reader changed what it read"
    )
