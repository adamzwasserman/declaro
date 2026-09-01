"""Steps for database/opening.feature. Real openers, no doubles.

`open_*` returns a value and connects to nothing, so every scenario below runs
without a server. The one that needs a primary names an address that is never
dialled: what is under test is what the VALUE decides, not what the network
does.
"""

from __future__ import annotations

import asyncio
import pathlib
from pathlib import Path
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum import close, is_replicated, open_sqlite, open_turso
from declaro_persistum.database import reading
from declaro_persistum.exceptions import DatabaseClosedError
from declaro_persistum.types import NOTHING

scenarios("../features/database/opening.feature")

# Never contacted. `open_turso` stores the primary and dials nothing, which is
# the property the first scenario asserts.
A_PRIMARY = "libsql://example.turso.io"


class Ctx(TypedDict, total=False):
    tmp: Path
    db: Any
    refused: Exception | None
    connected: bool
    run: Any


@pytest.fixture(scope="function")
def loop():
    made = asyncio.new_event_loop()
    asyncio.set_event_loop(made)
    yield made
    made.close()


@pytest.fixture
def ctx(loop) -> Ctx:
    return {"refused": None, "connected": False, "run": loop.run_until_complete}


@given("a temporary directory")
def _given_tmp(ctx, tmp_path: Path):
    ctx["tmp"] = tmp_path


def _refusal(ctx, make):
    """Run an opener and record a refusal rather than letting it escape.

    Narrowed to `ValueError` and `TypeError`: those are the two ways an opener
    says no to a policy it cannot honour or an argument it was not given. An
    OSError would mean it dialled something, which is a different scenario and
    must not be recorded as a refusal.
    """
    try:
        ctx["db"] = ctx["run"](make())
        ctx["refused"] = None
    except (ValueError, TypeError) as e:
        ctx["refused"] = e


@when("a local SQLite database is opened")
def _when_open_sqlite(ctx):
    path = ctx["tmp"] / "local.db"
    ctx["db"] = ctx["run"](open_sqlite(str(path), shutdown="exit_immediately", busy_timeout_s=5.0))
    ctx["connected"] = path.exists()


@then("the value is returned without a connection being made")
def _then_no_connection(ctx):
    assert ctx["db"] is not None
    assert not ctx["connected"], (
        "the file exists straight after `open_sqlite`, so the opener connected. "
        "The socket is meant to open at the first `reading` or `writing`, which "
        "is what lets a consumer construct a Database at import time."
    )


@then("it reports that it is not replicated")
def _then_not_replicated(ctx):
    assert is_replicated(ctx["db"]) is False, (
        f"a database with primary={ctx['db']['primary']!r} reports replicated"
    )


@when("a local SQLite database is opened without saying what shutdown means")
def _when_open_no_shutdown(ctx):
    _refusal(ctx, lambda: open_sqlite(str(ctx["tmp"] / "x.db"), busy_timeout_s=5.0))


@when("a local SQLite database is opened promising to replicate at shutdown")
def _when_open_bad_policy(ctx):
    """`replicate` is a real member of ShutdownPolicy. `open_sqlite` takes it.

    My first version of this passed `"replicate_then_exit"`, which is not in
    the vocabulary at all, and the opener took that too. That is how I learned
    it validates nothing rather than validating loosely.
    """
    _refusal(
        ctx,
        lambda: open_sqlite(str(ctx["tmp"] / "x.db"), shutdown="replicate", busy_timeout_s=5.0),
    )


@then("the opener refuses")
def _then_refuses(ctx):
    assert ctx["refused"] is not None, (
        "the opener accepted it. A shutdown policy has no default because the "
        "usual deployment has ephemeral disk."
    )


@then("it is refused, naming the primary it does not have")
def _then_refused_naming_the_primary(ctx):
    """FLIPPED 2026-08-17 by the note this step used to carry.

    It recorded that `open_sqlite` accepted "replicate", "exit_immediately"
    and "nonsense" alike, against what CLAUDE.md has always said, and it said
    it should fail the day someone added the validation and be renamed to the
    refusal. That day came, and this is the rename.

    The error names the primary because that is what the author has to supply
    or stop asking for.
    """
    refused = ctx["refused"]
    assert refused is not None, (
        "the opener accepted a promise to replicate from a database with no "
        "primary, so the failure would arrive at SIGTERM instead"
    )
    assert "primary" in str(refused).lower(), refused


@when("a Turso database is opened against a primary that is not there")
def _when_open_unreachable_primary(ctx):
    """The replicated opener dials at open time, by design.

    `open_turso` with a primary connects once and hands that one connection to
    every write. Opening one per write is declaro-dna: one sync handshake and
    one OS worker thread each, which took a consumer's box down at 20
    concurrent signups.
    """
    try:
        ctx["db"] = ctx["run"](
            open_turso(str(ctx["tmp"] / "r.db"), A_PRIMARY, "tok", shutdown="exit_immediately")
        )
        ctx["refused"] = None
    except Exception as e:
        ctx["refused"] = e


@then("the opener fails rather than returning a database that cannot sync")
def _then_open_fails_loudly(ctx):
    assert ctx["refused"] is not None, (
        "a database was returned for a primary that does not answer. Every "
        "write would then land locally and never reach it, which is the "
        "silence declaro-p39 was filed for."
    )
    assert "sync" in str(ctx["refused"]).lower() or "404" in str(ctx["refused"]), (
        f"the failure does not name the sync engine: {ctx['refused']!r}"
    )


@when("a local Turso database is opened")
def _when_open_turso_local(ctx):
    ctx["db"] = ctx["run"](
        open_turso(
            str(ctx["tmp"] / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING
        )
    )


@then("it carries no write lock, because nothing needs serialising")
def _then_no_lock(ctx):
    assert ctx["db"]["serialise"] is None, (
        "a local database carries a write lock. Serialising costs the "
        "concurrency MVCC exists to provide, and nothing local needs it."
    )


def _real_primary() -> tuple[str, str]:
    """The credentials in `.env`, or a skip.

    A REAL REPLICA, BECAUSE THE REPLICATED OPENER DIALS. `open_turso` with a
    primary connects during the call, so a made-up address does not produce a
    replicated Database, it produces a 404. The scenarios below are about what
    the resulting VALUE decides, and there is no way to get one without a
    primary that answers.

    Nothing here writes. The opener connects and sets the journal mode, both
    local operations, and the scenario reads two fields off the value.
    """
    env = pathlib.Path(__file__).resolve().parents[3] / ".env"
    if not env.exists():
        pytest.skip(f"no {env}, so there is no primary to open against")

    values = dict(
        line.split("=", 1)
        for line in env.read_text().splitlines()
        if line.strip() and not line.startswith("#") and "=" in line
    )
    url = values.get("TEST_TURSO_URL", "").strip().strip("\"'")
    token = values.get("TEST_TURSO_AUTH_TOKEN", "").strip().strip("\"'")
    if not url or not token:
        pytest.skip("TEST_TURSO_URL or TEST_TURSO_AUTH_TOKEN is not set in .env")
    return url, token


@when("a Turso database is opened with a primary")
def _when_open_turso_replicated(ctx):
    url, token = _real_primary()
    try:
        ctx["db"] = ctx["run"](
            open_turso(str(ctx["tmp"] / "r.db"), url, token, shutdown="exit_immediately")
        )
    except OSError as exc:
        pytest.skip(f"the primary at {url[:34]} is not reachable: {exc}")


@then("it reports that it is replicated")
def _then_replicated(ctx):
    assert is_replicated(ctx["db"]) is True, ctx["db"]["primary"]


@then("it carries no write lock, because one connection runs many writers")
def _then_replica_has_no_lock(ctx):
    """The reverse of what this step asserted until 2026-09-01.

    It required a lock, on the reading that a replica takes one connection so
    writes must go one at a time. The connection count is right and the
    conclusion is not: `BEGIN CONCURRENT` runs many writers on one connection.

    Measured against a real primary, 16 concurrent writers with no lock:
    nothing ever blocked, 17 of 17 durable locally and 17 of 17 on the
    primary, over four runs and once at 64 writers.
    """
    assert ctx["db"]["serialise"] is None, (
        "a replicated database still carries a write lock. It serialises "
        "writers that BEGIN CONCURRENT would have run at once, and it was "
        "built on a count of connections rather than a count of writes."
    )


@then("removing the primary changes the answer")
def _then_derived(ctx):
    """The predicate reads `primary`, so it cannot disagree with it."""
    assert is_replicated(ctx["db"]) is True

    without = {**ctx["db"], "primary": None}
    assert is_replicated(without) is False, (
        "removing the primary left the database reporting replicated, so the "
        "answer is stored somewhere else and the two can drift apart"
    )


@then("nothing else has to be updated to keep them agreeing")
def _then_single_source(ctx):
    """No second flag. A stored boolean is a copy that can go stale."""
    flags = [k for k, v in ctx["db"].items() if isinstance(v, bool) and "replic" in k.lower()]

    assert flags == [], (
        f"the Database carries {flags} beside `primary`. Two places to say one "
        f"thing is two places to get it wrong."
    )


@when("a local SQLite database is opened and then closed")
def _when_open_then_close(ctx):
    ctx["db"] = ctx["run"](
        open_sqlite(
            str(ctx["tmp"] / "closed.db"),
            shutdown="exit_immediately",
            busy_timeout_s=5.0,
        )
    )
    ctx["db"] = ctx["run"](close(ctx["db"]))


@then("asking it to read raises rather than opening a new connection")
def _then_closed_refuses(ctx):
    async def read() -> None:
        async with reading(ctx["db"]) as conn:
            await conn.execute("SELECT 1")

    with pytest.raises(DatabaseClosedError):
        ctx["run"](read())
