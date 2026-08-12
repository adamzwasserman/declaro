"""Tests for non-blocking initial replication on TursoPool.

Opening a cloud-backed pool used to await push() then pull() inline every
time, so every open paid a network round trip — measured downstream at
~1.35s against an existing small replica, dominating request latency once
pools were re-opened after idle eviction.

The sync is now backgrounded when a populated local replica already exists.
When it does not, there is nothing to serve, so the sync is awaited inline
rather than handing out a pool that reads an empty database and reports
success.

Callers needing a primary-consistent view await initial_pull_complete().
"""

import asyncio
import contextlib

import pytest

from declaro_persistum.pool import TursoPool


class _FakeHolder:
    """Stands in for _TursoConnectionHolder, recording sync calls."""

    def __init__(self, pull_delay: float = 0.0, pull_error: Exception | None = None):
        self.conn = object()
        self.calls: list[str] = []
        self._pull_delay = pull_delay
        self._pull_error = pull_error

    async def connect_async(self) -> None:
        self.calls.append("connect")

    async def push(self) -> None:
        self.calls.append("push")

    async def pull(self) -> None:
        if self._pull_delay:
            await asyncio.sleep(self._pull_delay)
        self.calls.append("pull")
        if self._pull_error:
            raise self._pull_error


def _pool(tmp_path, *, background_pull=True, holder=None, populated=True):
    """Build a cloud-configured pool with its holder and push loop stubbed."""
    db = tmp_path / "replica.db"
    if populated:
        db.write_bytes(b"x" * 64)

    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io",
        auth_token="tok",
        background_pull=background_pull,
    )
    pool._write_holder_stub = holder or _FakeHolder()  # type: ignore[attr-defined]
    return pool, db


async def _initialize(pool, holder):
    """Run _initialize with the holder and side effects stubbed out."""
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    orig_holder_cls = pool_mod._TursoConnectionHolder
    pool_mod._TursoConnectionHolder = lambda *_a, **_kw: holder  # type: ignore[assignment]

    # The push loop and FK enforcement are separate concerns; both would make
    # real calls here.
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    try:
        await pool._initialize()
    finally:
        pool_mod._TursoConnectionHolder = orig_holder_cls  # type: ignore[assignment]


class TestLocalReplicaDetection:
    """The decision is made from the filesystem, not assumed."""

    def test_populated_file_has_data(self, tmp_path):
        db = tmp_path / "r.db"
        db.write_bytes(b"x" * 32)
        pool = TursoPool(str(db), remote_url="https://x", background_pull=True)
        assert pool._local_replica_has_data() is True

    def test_missing_file_has_no_data(self, tmp_path):
        pool = TursoPool(str(tmp_path / "absent.db"), remote_url="https://x")
        assert pool._local_replica_has_data() is False

    def test_zero_byte_file_has_no_data(self, tmp_path):
        """A zero-byte file is not a usable replica."""
        db = tmp_path / "empty.db"
        db.write_bytes(b"")
        pool = TursoPool(str(db), remote_url="https://x")
        assert pool._local_replica_has_data() is False

    def test_unreadable_path_answers_false(self, tmp_path):
        """Errors answer False: a blocking replication costs latency, a wrong True
        serves empty results."""
        pool = TursoPool(str(tmp_path / "no" / "such" / "dir" / "r.db"), remote_url="https://x")
        assert pool._local_replica_has_data() is False


class TestOpenDoesNotBlock:
    """THE INVARIANT.

    A replication on pool open MUST NOT block the caller unless there is no
    local copy.

    When the local replica file already exists — every re-open, and the
    steady state on a persistent disk — the pool opens against the local
    copy immediately and syncs with cloud in the background. Only a genuine
    cold start with no local file may block to pull the initial copy,
    because there is nothing local to serve.

    A shared lock held across the initial replication broke this before 0.1.19: the
    open returned, but the first write queued behind the background sync, so
    the caller paid for it anyway. Downstream measured 474.9ms. These tests
    exist so that regression cannot return unnoticed.
    """

    @pytest.mark.asyncio
    async def test_existing_replica_does_not_await_pull(self, tmp_path):
        """_initialize returns before a slow pull finishes."""
        holder = _FakeHolder(pull_delay=0.5)
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        await _initialize(pool, holder)

        # The pull is still in flight; open did not wait for it.
        assert "pull" not in holder.calls
        assert pool._initial_replication_task is not None

        await pool.initial_pull_complete()
        assert "pull" in holder.calls

    @pytest.mark.asyncio
    async def test_open_is_fast_relative_to_pull(self, tmp_path):
        """Open completes in a small fraction of the pull's duration."""
        holder = _FakeHolder(pull_delay=0.5)
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        loop = asyncio.get_running_loop()
        start = loop.time()
        await _initialize(pool, holder)
        elapsed = loop.time() - start

        assert elapsed < 0.1, f"open blocked for {elapsed:.3f}s on a 0.5s pull"
        await pool.initial_pull_complete()


class TestNoLocalReplicaBlocks:
    """Without local data, returning early would serve an empty database."""

    @pytest.mark.asyncio
    async def test_missing_replica_awaits_sync_inline(self, tmp_path):
        holder = _FakeHolder()
        pool, _ = _pool(tmp_path, holder=holder, populated=False)

        await _initialize(pool, holder)

        # Sync already completed by the time the pool was handed out.
        assert "pull" in holder.calls
        assert pool._initial_replication_task is None

    @pytest.mark.asyncio
    async def test_background_pull_false_awaits_inline(self, tmp_path):
        """The opt-out restores fully inline replicating even with local data."""
        holder = _FakeHolder()
        pool, _ = _pool(tmp_path, holder=holder, background_pull=False, populated=True)

        await _initialize(pool, holder)

        assert "pull" in holder.calls
        assert pool._initial_replication_task is None


class TestPushBeforePullOrdering:
    """Un-pushed local frames must be delivered before pull() overwrites them."""

    @pytest.mark.asyncio
    async def test_background_path_pushes_before_pulling(self, tmp_path):
        holder = _FakeHolder()
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        await _initialize(pool, holder)
        await pool.initial_pull_complete()

        assert holder.calls.index("push") < holder.calls.index("pull")

    @pytest.mark.asyncio
    async def test_inline_path_pushes_before_pulling(self, tmp_path):
        holder = _FakeHolder()
        pool, _ = _pool(tmp_path, holder=holder, populated=False)

        await _initialize(pool, holder)

        assert holder.calls.index("push") < holder.calls.index("pull")


class TestBarrier:
    """initial_pull_complete is the handle for consistency-sensitive callers."""

    @pytest.mark.asyncio
    async def test_barrier_waits_for_completion(self, tmp_path):
        holder = _FakeHolder(pull_delay=0.2)
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        await _initialize(pool, holder)
        assert "pull" not in holder.calls

        await pool.initial_pull_complete()
        assert "pull" in holder.calls

    @pytest.mark.asyncio
    async def test_barrier_is_reentrant(self, tmp_path):
        """Awaiting after completion returns immediately, not hangs."""
        holder = _FakeHolder()
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        await _initialize(pool, holder)
        await pool.initial_pull_complete()
        await asyncio.wait_for(pool.initial_pull_complete(), timeout=1.0)

    @pytest.mark.asyncio
    async def test_local_only_pool_barrier_returns_immediately(self, tmp_path):
        """No remote_url means no sync to wait for."""
        pool = TursoPool(str(tmp_path / "local.db"))
        await asyncio.wait_for(pool.initial_pull_complete(), timeout=1.0)


class TestFailureHandling:
    """A failed background sync must not kill the pool, but must be reportable."""

    @pytest.mark.asyncio
    async def test_background_failure_does_not_raise_on_open(self, tmp_path):
        holder = _FakeHolder(pull_error=RuntimeError("network down"))
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        await _initialize(pool, holder)  # must not raise

        assert pool._initial_replication_task is not None

    @pytest.mark.asyncio
    async def test_barrier_reraises_background_failure(self, tmp_path):
        """A caller that asked for consistency is told it did not get it."""
        holder = _FakeHolder(pull_error=RuntimeError("network down"))
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        await _initialize(pool, holder)

        with pytest.raises(RuntimeError, match="network down"):
            await pool.initial_pull_complete()

    @pytest.mark.asyncio
    async def test_failed_sync_still_sets_the_event(self, tmp_path):
        """The barrier must never hang because the sync errored."""
        holder = _FakeHolder(pull_error=RuntimeError("boom"))
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        await _initialize(pool, holder)

        with pytest.raises(RuntimeError):
            await asyncio.wait_for(pool.initial_pull_complete(), timeout=1.0)


class TestMigrationsAwaitTheBarrier:
    """The one call site that must not see a stale replica.

    Introspecting a replica that has not caught up makes the differ compute
    against a schema that is not the primary's, and emit operations that
    correct code then faithfully applies. This is the same stale-input class
    as the foreign-key defect fixed in 0.1.10.
    """

    @pytest.mark.asyncio
    async def test_barrier_awaited_before_any_database_read(self, tmp_path):
        """apply_migrations_async waits before it touches the database."""
        from declaro_persistum.migrations import apply_migrations_async

        order: list[str] = []

        class _Pool:
            async def initial_pull_complete(self):
                order.append("barrier")

            def acquire(self):
                order.append("acquire")
                raise AssertionError("stop after the first acquire")

            def acquire_write(self, _concurrent=True):
                order.append("acquire")
                raise AssertionError("stop after the first acquire")

        schema = tmp_path / "models.py"
        schema.write_text("")

        with contextlib.suppress(Exception):
            await apply_migrations_async(_Pool(), "postgresql", schema, force=True)

        assert order, "migration never touched the pool"
        assert order[0] == "barrier", f"database read before the barrier: {order}"

    @pytest.mark.asyncio
    async def test_pool_without_the_barrier_still_migrates(self, tmp_path):
        """Non-Turso pools have no such method and must not break."""
        from declaro_persistum.migrations import apply_migrations_async

        touched: list[str] = []

        class _PlainPool:
            def acquire(self):
                touched.append("acquire")
                raise AssertionError("stop here")

        schema = tmp_path / "models.py"
        schema.write_text("")

        with contextlib.suppress(Exception):
            await apply_migrations_async(_PlainPool(), "postgresql", schema, force=True)

        assert touched == ["acquire"]


class TestTheInvariantEndToEnd:
    """One test per half of the invariant, named for what it protects."""

    @pytest.mark.asyncio
    async def test_warm_reopen_never_waits_on_the_network(self, tmp_path):
        """A local copy exists, so nothing about open may touch the cloud.

        Both the push and the pull are made slow. If open awaits either, the
        elapsed time gives it away.
        """
        holder = _FakeHolder(pull_delay=1.0)

        async def _slow_push():
            await asyncio.sleep(1.0)
            holder.calls.append("push")

        holder.push = _slow_push  # type: ignore[method-assign]
        pool, _ = _pool(tmp_path, holder=holder, populated=True)

        loop = asyncio.get_running_loop()
        start = loop.time()
        await _initialize(pool, holder)
        elapsed = loop.time() - start

        assert elapsed < 0.1, (
            f"a warm re-open blocked for {elapsed:.3f}s against a 1.0s sync. "
            f"THE INVARIANT: with a local copy present, open must not wait on "
            f"the cloud."
        )
        await pool.initial_pull_complete()

    @pytest.mark.asyncio
    async def test_cold_open_does_wait_because_nothing_is_local(self, tmp_path):
        """No local copy, so blocking is correct — there is nothing to serve.

        Returning early here would hand back a pool that reads an empty
        database and reports success.
        """
        holder = _FakeHolder()
        pool, _ = _pool(tmp_path, holder=holder, populated=False)

        await _initialize(pool, holder)

        assert "pull" in holder.calls, (
            "a cold open returned without pulling; the pool would serve an "
            "empty database and report success"
        )
        assert pool._initial_replication_task is None
