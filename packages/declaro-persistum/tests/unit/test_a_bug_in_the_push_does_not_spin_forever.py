"""A defect in the push path must not become an infinite loop at shutdown.

`flush` is the deliberate durability call, and it loops without a bound:

    while not await db["replicate_once"](db):
        await db["sleep"](db["retry_delay_s"])

That is right for a network failure. The caller has said it is willing to wait,
and the primary will come back.

IT IS WRONG FOR A BUG OF OURS. `replicate_once` catches `Exception` and returns
False, so a `TypeError` from calling `push` wrongly returns False too, and
`flush` retries it for ever. `close(db)` calls `flush`, so a replicated
database with a defect in its push path hangs the process at shutdown instead
of reporting anything.

The existing comment shows the shape has already cost something once: it
records a loop logging "attempt 1247 failed after 3021s" without being able to
say why. A retry that will never succeed is not patience, it is a hang.

EVERY TEST HERE IS BOUNDED. A control for this failure mode does not fail, it
spins, and an unbounded one costs a session. `asyncio.wait_for` closes each
one.
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest

from declaro_persistum.database import (
    Database,
    flush,
    no_passes,
    nothing_was_trapped,
)
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE
from declaro_persistum.write_queue import PendingWrite  # noqa: F401  (documents the shape)

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit]


async def never_used(*_args: object, **_kwargs: object) -> object:
    """A door this test never opens, so reaching it is a mistake worth hearing.

    ONE FUNCTION RETURNING None STOOD IN FOR FOUR KEYS HERE. Three of them --
    connect, for_ddl, write_one -- are never called, because this test is
    about the retry loop inside `flush`. A None made "nothing to do" and "not
    written yet" identical, and would have made a wrong call look like a
    working one.

    The fourth, `close_connection`, and the pass counters take the package's
    own answers: `no_passes` returns 0 because zero passes ran, which is a
    true answer rather than an absence.
    """
    raise AssertionError(
        "this test reached a door it does not exercise; either the test or "
        "the code under it has changed shape"
    )


def _replicated(replicate_once, sleeps: list[float]):
    """A replicated Database whose push behaviour is the argument.

    `flush` only loops when `is_replicated` is true, which is derived from
    `primary`, so the primary has to be set. Nothing here dials it.
    """

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) > 50:
            raise AssertionError(
                f"flush retried {len(sleeps)} times without stopping. A retry "
                f"that will never succeed is a hang, not patience."
            )


    return Database(
        path="/tmp/unused.db",
        dialect="turso",
        journal_mode="wal",
        busy_timeout_s=5.0,
        primary="libsql://example.invalid",
        token="t",
        connect=never_used,
        close_connection=no_passes,
        for_ddl=never_used,
        serialise=None,
        shutdown="exit_immediately",
        write_one=never_used,
        replicate_once=replicate_once,
        refresh_once=no_passes,
        release=no_passes,
        sleep=sleep,
        retry_delay_s=0.0,
        closed=False,
        untrap=nothing_was_trapped,
    )


async def test_flush_returns_once_the_push_lands() -> None:
    """The ordinary path, so the loop below means something."""
    attempts = []

    async def eventually(_db) -> bool:
        attempts.append(1)
        return len(attempts) >= 3

    sleeps: list[float] = []
    await asyncio.wait_for(flush(_replicated(eventually, sleeps)), timeout=5)

    assert len(attempts) == 3, attempts
    assert len(sleeps) == 2, "flush slept a different number of times than it retried"


async def test_flush_keeps_retrying_a_network_failure() -> None:
    """The behaviour that must survive. A primary comes back; we wait for it."""
    attempts = []

    async def offline(_db) -> bool:
        attempts.append(1)
        return len(attempts) >= 10

    sleeps: list[float] = []
    await asyncio.wait_for(flush(_replicated(offline, sleeps)), timeout=5)

    assert len(attempts) == 10


async def test_a_bug_of_ours_escapes_the_loop_rather_than_spinning() -> None:
    """The defect, bounded so it fails rather than hangs.

    `replicate_once` in `turso_database` catches Exception and returns False.
    A TypeError from calling `push` wrongly is not a network failure and will
    never succeed, so it must leave the loop.
    """
    async def our_bug(_db) -> bool:
        raise TypeError("push() got an unexpected keyword argument")

    sleeps: list[float] = []
    with pytest.raises(TypeError):
        await asyncio.wait_for(flush(_replicated(our_bug, sleeps)), timeout=5)

    assert sleeps == [], "flush slept before letting our own bug out"


# ---------------------------------------------------------------------------
# The real closure, which is where the swallow lives
# ---------------------------------------------------------------------------


def _credentials():
    env = pathlib.Path(__file__).resolve().parents[2] / ".env"
    if not env.exists():
        pytest.skip("no .env, so there is no primary to open against")
    values = dict(
        line.split("=", 1)
        for line in env.read_text().splitlines()
        if line.strip() and not line.startswith("#") and "=" in line
    )
    url = values.get("TEST_TURSO_URL", "").strip().strip("\"'")
    token = values.get("TEST_TURSO_AUTH_TOKEN", "").strip().strip("\"'")
    if not url or not token:
        pytest.skip("TEST_TURSO_URL or TEST_TURSO_AUTH_TOKEN is not set")
    return url, token


@pytest.mark.turso
@pytest.mark.network
async def test_the_real_replicate_once_lets_our_bug_out(tmp_path) -> None:
    """Against a live replica, because the swallow is inside `open_turso`.

    The closure is built during the replicated open and cannot be reached any
    other way. Nothing is written to the primary: `push` is replaced before it
    is ever called.
    """
    from declaro_persistum import close, open_turso

    url, token = _credentials()
    try:
        db = await open_turso(
            str(tmp_path / "r.db"), url, token, shutdown="exit_immediately"
        )
    except OSError as exc:
        pytest.skip(f"the primary is not reachable: {exc}")

    held = await db["connect"](db)
    working_push = held.push

    async def broken_push():
        raise TypeError("push() got an unexpected keyword argument")

    try:
        held.push = broken_push

        with pytest.raises(A_BUG_IN_THIS_PACKAGE):
            await asyncio.wait_for(db["replicate_once"](db), timeout=10)
    finally:
        # PUT THE REAL METHOD BACK BEFORE CLOSING. `close` calls `flush`, which
        # calls `replicate_once`, which now correctly raises -- so leaving the
        # broken one in place makes the cleanup raise and the test fail after
        # its assertion has already passed. Before the fix it did something
        # worse: `close` retried the broken push until pytest's timeout, which
        # is the hang this test exists for.
        held.push = working_push
        await close(db)
