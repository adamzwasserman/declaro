"""Steps for replication.feature.

A feature file with no step definitions is documentation, not a test. Two files
on this branch — materialized_views (20 scenarios) and multi_backend (5) — are
bound by no `scenarios()` call at all and have never executed. These run.

The primary is an injected recorder rather than a live Turso Cloud database:
what is under test is persistum's ordering, politeness, retry, trapping and
logging. Routing it through the network would be measuring Turso instead. Where
the ENGINE's behaviour is the claim — a cold open copying the whole database —
the scenario says so, and the number in it came from a measurement against a
real primary rather than from this file.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import signal
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum import database as db_mod
from declaro_persistum.database import (
    Database,
    new_database,
    new_write_lock,
    replicate,
    replicate_if_idle,
    replicate_on_shutdown,
    writers_waiting,
    writing,
)
from declaro_persistum.turso_database import open_turso, replicate_path

scenarios("../features/replication.feature")


class Ctx(TypedDict, total=False):
    db: Database
    calls: list[str]
    logs: list[str]
    error: Exception | None
    path: str


@pytest.fixture
def ctx() -> Ctx:
    return {}


class _Recorder:
    """Records the ORDER of both directions, which is the property under test."""

    def __init__(self, *, batches: int = 0, failures: int = 0) -> None:
        self.calls: list[str] = []
        self.batches = batches
        self.failures = failures
        self.closed = False

    async def commit(self) -> None:
        # `writing(db)` finishes a block by committing, so a stand-in for a
        # connection has to be able to. Deliberately NOT recorded: `calls` is
        # what these scenarios read to detect REMOTE I/O on a caller path, and
        # a commit is local. Recording it here made "no read or write waits
        # for replication" fail on a local commit.
        return None

    async def rollback(self) -> None:
        return None

    async def push(self) -> None:
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionResetError("primary unreachable")
        self.calls.append("up")

    async def pull(self) -> bool:
        self.calls.append("down")
        if self.batches > 0:
            self.batches -= 1
            return True
        return False

    async def close(self) -> None:
        self.closed = True


async def _write_one(conn, sql, params):
    await conn.execute(sql, params)


def _make(tmp_path, *, replicated=True, recorder=None, shutdown="exit_immediately"):
    """A Database whose replication callables are the recorder, and nothing else."""
    rec = recorder or _Recorder()

    async def connect(db: Database) -> Any:
        return rec

    async def close_connection(conn: Any) -> None:
        return None

    async def replicate_once(db: Database) -> bool:
        try:
            await rec.push()
            return True
        except Exception:
            return False

    async def refresh_once(db: Database) -> None:
        while await rec.pull():
            pass

    async def release(db: Database) -> None:
        return None

    async def sleep(_s: float) -> None:
        return None

    db = new_database(
        path=str(tmp_path / "copy.db"),
        dialect="sqlite",
        journal_mode="wal",
        busy_timeout_s=5.0,
        primary="https://example.turso.io" if replicated else None,
        token="t" if replicated else None,
        connect=connect,
        close_connection=close_connection,
        for_ddl=writing,
        serialise=new_write_lock(asyncio.Lock()) if replicated else None,
        shutdown=shutdown,
        write_one=_write_one,
        replicate_once=replicate_once,
        refresh_once=refresh_once,
        release=release,
        sleep=sleep,
        retry_delay_s=0.001,
    )
    return db, rec


# ---------------------------------------------------------------- the function


@given("a local copy and its primary")
@given("a replicated database whose background replication is running")
@given("a replicated database under sustained write load")
@given("a replicated database with no pending writes and no waiters")
@given("a replicated database serving reads and writes")
@given("a shutdown in progress with local commits the primary has not seen")
@given("a shutdown that is taking a long time")
@given("a platform grace period shorter than the replication needs")
@given("a caller that needs the primary current before it continues")
@given("a replicated database whose caller asked for shutdown replication")
@given("a caller that has not asked for shutdown replication")
@given("a database with no primary")
@given("a local copy that already exists")
@given("no local copy")
def _given_context(ctx, tmp_path):
    ctx["tmp_path"] = tmp_path


@when("replicate runs")
def _when_replicate_runs(ctx):
    async def run():
        db, rec = _make(ctx["tmp_path"], recorder=_Recorder(batches=2))
        await replicate(db)
        ctx["calls"] = list(rec.calls)

    asyncio.run(run())


@then(
    "local commits the primary has not seen go up first, and only then do the primary's changes come down, so a write waiting since the last shutdown is never overwritten by what follows it"
)
def _then_up_before_down(ctx):
    calls = ctx["calls"]
    assert "up" in calls, "replication sent nothing to the primary"
    assert "down" in calls, (
        "replication brought nothing down; a copy that only sends up learns "
        "nothing about its primary for the life of the process"
    )
    assert calls.index("up") < calls.index("down"), (
        f"the primary's changes came down before local commits went up: {calls}"
    )


@then(
    'it keeps bringing changes down until the primary reports nothing further, because a single pull fetches one batch and returns "there may be more"'
)
def _then_converges(ctx):
    """Driven against the REAL convergence loop, not the recorder.

    The rest of this scenario runs against a recorder standing in for the
    engine, which is right for ORDER — up-before-down is persistum's decision.
    Convergence is not: it lives in `_pull_until_level`. Asserting it against a
    double whose own `refresh_once` loops would be asserting the double, and a
    mutation that removed the real loop survived exactly that way.
    """
    from declaro_persistum.turso_database import _pull_until_level

    class _Batched:
        def __init__(self, batches):
            self.remaining = batches
            self.calls = 0

        async def pull(self):
            self.calls += 1
            if self.remaining:
                self.remaining -= 1
                return True
            return False

    conn = _Batched(2)
    assert asyncio.run(_pull_until_level(conn)) is True
    assert conn.calls == 3, (
        f"two batches were offered and pull was called {conn.calls} time(s); "
        f"convergence needs one per batch plus the one that reports empty. A "
        f"copy more than one batch behind is otherwise left part-way and "
        f"reported current."
    )


@when("replicate is asked for on a path with no primary")
def _when_local_only(ctx):
    async def run():
        db, _ = _make(ctx["tmp_path"], replicated=False)
        try:
            await replicate(db)
            ctx["error"] = None
        except Exception as e:  # noqa: BLE001 - raising IS the behaviour
            ctx["error"] = e
        try:
            await replicate_path(str(ctx["tmp_path"] / "x.db"), primary="", token=None)
            ctx["path_error"] = None
        except Exception as e:  # noqa: BLE001
            ctx["path_error"] = e

    asyncio.run(run())


@then(
    "it raises, naming the database, because a local-only database has nothing to bring into conformity"
)
def _then_raises(ctx):
    assert ctx["error"] is not None, "a local-only database reported success"
    assert "no primary" in str(ctx["error"]), (
        f"the error does not say what is wrong: {ctx['error']}"
    )
    assert ctx["path_error"] is not None, "replicate_path accepted an empty primary"


@then(
    'the previous version returned True "by vacuity rather than by success", which a caller cannot tell apart from a replication that worked'
)
def _then_not_vacuous(ctx):
    assert not isinstance(ctx["error"], type(None)), "the vacuous True is back"


# --------------------------------------------------------------- opportunistic


@when("a replication pass is considered")
def _when_pass_considered(ctx):
    async def run():
        db, rec = _make(ctx["tmp_path"])
        ctx["ran_free"] = await replicate_if_idle(db)
        rec.calls.clear()
        db["serialise"]["waiting"] = 1
        ctx["ran_busy"] = await replicate_if_idle(db)
        ctx["calls_busy"] = list(rec.calls)
        ctx["db"] = db

    asyncio.run(run())


@then(
    "it proceeds only when no writer is waiting for the serialise lock, because replication goes out on the held connection and every waiting writer is a request paying for the round trip"
)
def _then_yields(ctx):
    assert ctx["ran_free"] is True, "replication deferred with nothing waiting"
    assert ctx["ran_busy"] is False, "replication ran with a writer waiting"
    assert ctx["calls_busy"] == [], (
        f"replication touched the connection anyway: {ctx['calls_busy']}"
    )


@then(
    "load is counted as writers waiting for that lock, NOT as CPU and NOT as active readers — a reader takes no lock at all and never contends with replication"
)
def _then_readers_not_load(ctx):
    async def run():
        db, _ = _make(ctx["tmp_path"])
        async with db_mod.reading(db):
            assert writers_waiting(db) == 0, "a reader was counted as load"

    asyncio.run(run())


@then(
    "it is the exact resource replication takes rather than a cheaper signal standing in for it, which is the substitution that has already cost this package three production incidents"
)
def _then_counter_is_real(ctx):
    async def run():
        db, _ = _make(ctx["tmp_path"])
        release = asyncio.Event()

        async def writer():
            async with writing(db):
                await release.wait()

        first = asyncio.create_task(writer())
        for _ in range(20):
            await asyncio.sleep(0)
        blocked = asyncio.create_task(writer())
        for _ in range(20):
            await asyncio.sleep(0)

        assert writers_waiting(db) == 1, (
            f"a genuinely blocked writer read as {writers_waiting(db)}; a "
            f"counter that is never incremented satisfies every test that "
            f"sets it by hand"
        )
        release.set()
        await asyncio.gather(first, blocked)
        assert writers_waiting(db) == 0, "the count did not come back down"

    asyncio.run(run())


@when("ten replication passes are considered while a writer waits")
def _when_ten_passes(ctx):
    async def run():
        db, rec = _make(ctx["tmp_path"])
        db["serialise"]["waiting"] = 1
        for _ in range(10):
            await replicate_if_idle(db)
        ctx["under_load"] = list(rec.calls)
        db["serialise"]["waiting"] = 0
        await replicate_if_idle(db)
        ctx["after_load"] = list(rec.calls)

    asyncio.run(run())


@then(
    "none of them run, and when the load clears exactly one pass runs rather than ten, because a deferred pass is dropped and not accumulated"
)
def _then_no_backlog(ctx):
    assert ctx["under_load"] == [], f"replication ran under load: {ctx['under_load']}"
    ups = [c for c in ctx["after_load"] if c == "up"]
    assert len(ups) == 1, (
        f"ten deferred passes released {len(ups)} when the load cleared"
    )


@when("time passes")
def _when_time_passes(ctx):
    ctx["params"] = inspect.signature(open_turso).parameters
    ctx["loop_params"] = inspect.signature(db_mod.replication_loop).parameters


@then("no timer wakes to take the connection and discover there is nothing to do")
def _then_no_clock(ctx):
    assert not any("interval" in p for p in ctx["params"]), (
        "an interval is back; replication is triggered by the connection being "
        "free, not by a clock"
    )
    assert "wanted" in ctx["loop_params"], (
        "the replication loop takes no trigger, so it can only be a timer"
    )


# --------------------------------------------------------------- non-blocking


@when("a read and a write run")
def _when_read_and_write(ctx):
    async def run():
        db, rec = _make(ctx["tmp_path"])
        rec.calls.clear()
        async with db_mod.reading(db):
            pass
        async with writing(db):
            pass
        ctx["calls"] = list(rec.calls)

    asyncio.run(run())


@then(
    "neither awaits a remote round trip, because a local commit is sub-millisecond and a cloud round trip is not"
)
def _then_no_remote_on_path(ctx):
    assert ctx["calls"] == [], f"a caller path performed remote I/O: {ctx['calls']}"


@when("the database is opened on it")
@when("the database is opened against a primary")
def _when_opened(ctx):
    async def run():
        path = str(ctx["tmp_path"] / "warm.db")
        db = await open_turso(path, shutdown="exit_immediately")
        ctx["opened"] = db
        ctx["is_local"] = db["primary"] is None

    asyncio.run(run())


@then(
    "the open does not replicate before returning, because the schema is already on disk and only DATA can be behind — which is the eventual consistency the caller asked for, not a weakening of it"
)
def _then_warm_open_quiet(ctx):
    src = inspect.getsource(open_turso)
    assert "await replicate" not in src, (
        "the open replicates inline; 0.3.4 did this and cost a consumer "
        "22.8 -> 16.6 ops/s to re-fetch a schema that was already local"
    )


@then(
    "the whole database is copied before the open returns, because a database with no schema is unusable rather than merely stale"
)
def _then_cold_open_bootstraps(ctx):
    assert ctx["opened"] is not None


@then(
    "that copy is the engine's, not persistum's — measured 2026-08-13 at 2.8s alone and 20-25s when 25 cold opens are issued at once, because they serialize"
)
def _then_bootstrap_is_engine(ctx):
    import turso.aio.sync

    sig = inspect.signature(turso.aio.sync.connect)
    assert sig.parameters["bootstrap_if_empty"].default is True, (
        "pyturso no longer bootstraps an empty copy by default, so a cold open "
        "would return a database with no schema"
    )


# ------------------------------------------------------------------- shutdown


@when("the process receives SIGTERM or SIGINT")
def _when_signal(ctx):
    fired: list[str] = []

    def _host_handler(*_a):
        fired.append("host")

    # signal.signal() returns the handler it REPLACED, not the one installed.
    # Comparing against that return value makes the assertion always true —
    # verified by mutation on the other branch, where it let two scenarios pass
    # with trapping disabled.
    previous = signal.signal(signal.SIGTERM, _host_handler)
    try:
        from declaro_persistum.shutdown import trap_shutdown

        async def run():
            db, rec = _make(ctx["tmp_path"], shutdown="replicate")
            trap_shutdown(db)
            installed = signal.getsignal(signal.SIGTERM)
            ctx["installed_differs"] = installed is not _host_handler
            installed(signal.SIGTERM, None)
            await asyncio.sleep(0)
            ctx["host_fired"] = fired == ["host"]

        asyncio.run(run())
    finally:
        signal.signal(signal.SIGTERM, previous)


@then("replication runs to completion before the process exits")
def _then_trapped(ctx):
    assert ctx["installed_differs"], "no shutdown handler was installed"


@then(
    "any handler already installed by the host application still runs, because a library that silently replaces a caller's signal handling breaks the program it is serving"
)
def _then_chained(ctx):
    assert ctx["host_fired"], (
        "the host application's SIGTERM handler was replaced rather than "
        "chained, so its own shutdown work never runs"
    )


@when("a database is opened")
def _when_opened_untrapped(ctx):
    before = signal.getsignal(signal.SIGTERM)

    async def run():
        db, _ = _make(ctx["tmp_path"], shutdown="exit_immediately")
        from declaro_persistum.shutdown import trap_shutdown

        trap_shutdown(db)
        ctx["unchanged"] = signal.getsignal(signal.SIGTERM) is before
        ctx["default"] = inspect.signature(open_turso).parameters["shutdown"].default

    asyncio.run(run())


@then(
    "no signal handler is installed, because installing one behind a caller's back changes the behaviour of a program that never opted into it"
)
def _then_untrapped(ctx):
    assert ctx["unchanged"], (
        'a database asked for "exit_immediately" installed a handler anyway'
    )


@then(
    'the shutdown policy is a REQUIRED argument with no default, because a default cannot tell "chose this" from "never knew there was a choice" — and on ephemeral disk a default would silently pick the losing side of the exact failure it exists to prevent'
)
def _then_no_default(ctx):
    assert ctx["default"] is inspect.Parameter.empty, (
        "shutdown has a default; a caller who never thought about unreplicated "
        "writes would be assigned an answer instead of being made to give one"
    )


@when("shutdown replication runs")
@when("shutdown replication is running")
@when("the process is killed before replication completes")
def _when_shutdown(ctx, caplog):
    async def run():
        db, rec = _make(ctx["tmp_path"], recorder=_Recorder(failures=1))
        db["serialise"]["waiting"] = 5  # heavy load: must be ignored
        clock = iter([0.0, 1.5, 3.0, 4.5, 6.0, 7.5, 9.0])
        with caplog.at_level(logging.INFO):
            await replicate_on_shutdown(db, now=lambda: next(clock))
        ctx["calls"] = list(rec.calls)
        ctx["logs"] = [r.getMessage() for r in caplog.records]
        ctx["path"] = db["path"]

    asyncio.run(run())


@then(
    "it ignores load entirely and takes the connection, because the politeness that is correct during service is data loss during shutdown"
)
def _then_shutdown_ignores_load(ctx):
    assert "up" in ctx["calls"], (
        "shutdown deferred to load and exited without replicating; on "
        "ephemeral disk those writes are gone"
    )


@then(
    "it blocks until the primary has every local commit, because on ephemeral disk anything not replicated when the process dies is gone"
)
def _then_shutdown_blocks(ctx):
    assert "down" in ctx["calls"], "shutdown did not complete both directions"


@then(
    "it logs when it starts and keeps logging as it goes, so an operator watching a process refuse to exit can see it is working rather than hung"
)
def _then_shutdown_logs(ctx):
    joined = " ".join(ctx["logs"]).lower()
    assert "replicat" in joined, "shutdown replication logged nothing"
    assert len(ctx["logs"]) >= 2, (
        f"only one line logged, so there is no progress to watch: {ctx['logs']}"
    )


@then(
    'every line names the database and the elapsed time, because "replicating" alone does not distinguish progress from a stall, and does not say WHICH database is holding up the exit'
)
def _then_lines_name_db_and_time(ctx):
    for line in ctx["logs"]:
        assert ctx["path"] in line, f"line does not name the database: {line}"
    assert any("s." in m or "s (" in m for m in ctx["logs"]), (
        f"no elapsed time in the shutdown log: {ctx['logs']}"
    )


@then(
    "the last line logged states that the data has not yet reached the primary, because the platform can always cut this short and a silent truncation is indistinguishable from success"
)
def _then_truncation_visible(ctx):
    assert any("Not yet delivered" in m for m in ctx["logs"]), (
        f"a retry said nothing about what is still undelivered, so a SIGKILL "
        f"mid-replication would leave no record: {ctx['logs']}"
    )


# ------------------------------------------------------------------- explicit


@when("it calls flush and awaits it")
def _when_flush(ctx):
    async def run():
        db, rec = _make(ctx["tmp_path"])
        db["serialise"]["waiting"] = 3  # an explicit call ignores load
        await db_mod.flush(db)
        ctx["calls"] = list(rec.calls)

    asyncio.run(run())


@then(
    "it returns once both directions have completed, and it ignores load, because a caller that awaited this has already said it is willing to wait"
)
def _then_flush_completed(ctx):
    assert "up" in ctx["calls"] and "down" in ctx["calls"], (
        f"flush did not complete both directions: {ctx['calls']}"
    )


@given("a path with no local copy and a primary")
def _given_no_copy(ctx, tmp_path):
    ctx["tmp_path"] = tmp_path
    ctx["path"] = str(tmp_path / "users" / "new" / "app.db")


@when("replicate is asked for on that path")
def _when_replicate_path(ctx, monkeypatch):
    rec = _Recorder()
    made: list[str] = []

    class _FakeSync:
        @staticmethod
        async def connect(path, remote_url=None, auth_token=None):
            made.append(path)
            return rec

    import turso.aio

    monkeypatch.setattr(turso.aio, "sync", _FakeSync, raising=False)
    import sys

    monkeypatch.setitem(sys.modules, "turso.aio.sync", _FakeSync)

    asyncio.run(
        replicate_path(ctx["path"], primary="https://example.turso.io", token="t")
    )
    ctx["made"] = made
    ctx["calls"] = list(rec.calls)
    ctx["closed"] = rec.closed


@then("the local copy is created and filled from the primary, and the call succeeds")
def _then_created(ctx):
    assert ctx["made"] == [ctx["path"]], (
        f"replicate_path opened nothing at that path: {ctx['made']}"
    )
    assert "down" in ctx["calls"], (
        f"the new copy was never filled from the primary: {ctx['calls']}"
    )


@then(
    "this is how a database is provisioned before anyone opens it — the same verb whether or not a copy exists yet, because a caller should not have to know which case it is in to ask for the same outcome"
)
def _then_same_verb(ctx):
    assert ctx["calls"].index("up") < ctx["calls"].index("down"), (
        f"the provisioning path brought changes down before sending up: "
        f"{ctx['calls']}"
    )
    assert ctx["closed"], (
        "replicate_path left its connection open; it must neither disturb nor "
        "depend on an open Database"
    )
