"""
Durability regression tests for the Turso embedded-replica sync path.

Covers the FK-push silent-data-loss bug (BUG_REPORT_fk_push_silent_data_loss.md)
and its defense-in-depth remediation (W1-W5).

The pyturso replica connection + network is replaced by an in-process fake holder
(_FakeHolder) so the pool's *orchestration* logic — the code under repair — is
tested deterministically with no cloud DB. The real-cloud enforcement mechanism
for W1 (PRAGMA foreign_keys = ON causing an IntegrityError at commit) was proven
directly against the production libsql engine; these tests lock in the pool-side
contract that drives it.
"""

import pytest

import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py
from declaro_persistum.pool import TursoPool, _TursoConnectionHolder


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.description = None
        self.rowcount = 0

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    """In-process stand-in for a pyturso replica connection."""

    def __init__(self):
        self.executed: list[tuple[str, tuple]] = []
        self.commits = 0
        self.foreign_keys = False

    async def execute(self, sql, parameters=()):
        self.executed.append((sql, parameters))
        s = sql.strip().lower().replace(" ", "")
        if s.startswith("pragmaforeign_keys=on"):
            self.foreign_keys = True
            return _FakeCursor([])
        if s.startswith("pragmaforeign_keys"):
            return _FakeCursor([(1 if self.foreign_keys else 0,)])
        if s.startswith("pragmajournal_mode"):
            return _FakeCursor([("wal",)])
        return _FakeCursor([])

    async def commit(self):
        self.commits += 1

    async def close(self):
        pass


class _FakeHolder(_TursoConnectionHolder):
    """Fake sync holder: records push/pull ordering, can fail pushes."""

    # Class-level knobs the tests set before _initialize runs.
    push_error: Exception | None = None
    # Ordering across connections. A cloud pool pushes on its dedicated push
    # connection and pulls on the write connection, so push-before-pull is an
    # ordering between two holders and cannot be seen from either alone.
    shared_events: list[str] = []

    def __init__(self, database_path, remote_url=None, auth_token=None):
        super().__init__(database_path, remote_url, auth_token)
        self.events: list[str] = []
        self.push_count = 0
        self.pull_count = 0

    async def connect_async(self):
        self.conn = _FakeConn()

    async def push(self):
        self.events.append("push")
        type(self).shared_events.append("push")
        self.push_count += 1
        if type(self).push_error is not None:
            raise type(self).push_error

    async def pull(self):
        self.events.append("pull")
        type(self).shared_events.append("pull")
        self.pull_count += 1


@pytest.fixture
def fake_holder(monkeypatch):
    """Patch the pool to build _FakeHolder instances; return the live instance."""
    created = {}

    created["holders"] = []

    def factory(database_path, remote_url=None, auth_token=None):
        h = _FakeHolder(database_path, remote_url, auth_token)
        created["holders"].append(h)
        # The first holder built is the write holder. A cloud pool also opens
        # a second, dedicated push connection, so these tests must keep
        # pointing at the write holder rather than at whichever was made last.
        created.setdefault("holder", h)
        return h

    _FakeHolder.push_error = None
    _FakeHolder.shared_events = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", factory)
    return created


class TestW1ForeignKeyEnforcementOnReplica:
    """W1: the replica connection must enforce FKs so a violating write fails
    fast at commit rather than committing locally and being lost on re-replicate."""

    @pytest.mark.asyncio
    async def test_remote_init_enables_foreign_keys(self, fake_holder):
        pool = TursoPool("/tmp/does-not-matter.db", remote_url="https://x.turso.io",
                         auth_token="t")
        await pool._initialize()
        holder = fake_holder["holder"]
        assert holder.conn.foreign_keys is True, (
            "replica connection must have PRAGMA foreign_keys = ON after init — "
            "otherwise FK-violating writes commit locally and are silently lost"
        )
        await pool.close()


async def _connect_no_loop(pool, path, remote_url, token):
    """Attach a connected _FakeHolder without starting the background push loop."""
    holder = _FakeHolder(path, remote_url, token)
    await holder.connect_async()
    pool._write_holder = holder
    # The push runs on its own connection and no longer borrows the write
    # connection, so a test that exercises pushes must supply one.
    push_holder = _FakeHolder(path, remote_url, token)
    await push_holder.connect_async()
    pool._push_holder = push_holder
    return holder


class TestW2SurfacePushFailures:
    """W2: a push that keeps failing must be observable by the application —
    not merely logged at WARNING — so it can learn a committed write is not
    durable."""

    @pytest.mark.asyncio
    async def test_push_failure_sets_last_error_and_fires_callback(self):
        _FakeHolder.push_error = RuntimeError("FOREIGN KEY constraint failed")
        pool = TursoPool("/tmp/x.db", remote_url="https://x.turso.io", auth_token="t")
        await _connect_no_loop(pool, "/tmp/x.db", "https://x.turso.io", "t")

        seen = []
        pool.set_push_failure_callback(lambda err, count: seen.append((str(err), count)),
                                       threshold=2)

        assert await pool._push_once() is False
        assert pool.last_push_error is not None       # first failure recorded
        assert seen == []                              # below threshold, no callback yet

        assert await pool._push_once() is False        # crosses threshold=2
        assert len(seen) == 1
        assert "FOREIGN KEY" in seen[0][0]
        assert seen[0][1] == 2
        _FakeHolder.push_error = None

    @pytest.mark.asyncio
    async def test_push_success_clears_error_state(self):
        _FakeHolder.push_error = RuntimeError("transient")
        pool = TursoPool("/tmp/x.db", remote_url="https://x.turso.io", auth_token="t")
        await _connect_no_loop(pool, "/tmp/x.db", "https://x.turso.io", "t")

        await pool._push_once()
        assert pool.last_push_error is not None
        assert pool.push_healthy is False

        _FakeHolder.push_error = None
        assert await pool._push_once() is True
        assert pool.last_push_error is None
        assert pool.push_healthy is True


class TestW3ProtectUnpushedFramesOnResync:
    """W3: bootstrap/re-replicate pull() must not silently overwrite writes that a
    prior process committed locally but never pushed. Deliver them first."""

    @pytest.mark.asyncio
    async def test_initialize_pushes_before_pull(self, fake_holder):
        pool = TursoPool("/tmp/x.db", remote_url="https://x.turso.io",
                         auth_token="t", push_interval_s=1000)
        await pool._initialize()
        holder = fake_holder["holder"]
        # Read across connections: init pushes on the push connection and
        # pulls on the write connection.
        init_events = list(_FakeHolder.shared_events)
        await pool.close()
        assert init_events == ["push", "pull"], (
            "init must push un-pushed local frames BEFORE pull() overwrites them; "
            f"got {init_events}"
        )

    @pytest.mark.asyncio
    async def test_refresh_connections_pushes_before_pull(self, fake_holder):  # noqa: ARG002 - fixture patches the holder factory
        pool = TursoPool("/tmp/x.db", remote_url="https://x.turso.io",
                         auth_token="t", push_interval_s=1000)
        await pool._initialize()
        _FakeHolder.shared_events.clear()
        await pool.refresh_connections()
        refresh_events = list(_FakeHolder.shared_events)
        await pool.close()
        # Read across both connections: a cloud pool pushes on its dedicated
        # push connection and pulls on the write connection, so the ordering
        # this guards is no longer visible from either holder alone.
        assert refresh_events.index("push") < refresh_events.index("pull"), (
            "refresh_connections must push before pull; got " f"{refresh_events}"
        )


class TestW5HonestMigrationSkips:
    """W5: when reconstruction ops are skipped on a replica, the caller must be
    able to see WHAT was skipped — not mistake a silent no-op for success."""

    def test_partition_reports_skipped_reconstruction_ops(self):
        from declaro_persistum.migrations import partition_replica_operations

        operations = [
            {"op": "create_table", "table": "parent"},
            {"op": "add_foreign_key", "table": "child"},
            {"op": "add_column", "table": "child"},
        ]
        execution_order = [0, 1, 2]

        safe_order, skipped = partition_replica_operations(operations, execution_order)

        # add_foreign_key requires reconstruction -> deferred; others are safe.
        assert 1 not in safe_order
        assert safe_order == [0, 2]
        assert {"op": "add_foreign_key", "table": "child"} in skipped
        assert len(skipped) == 1

    def test_partition_all_safe_yields_no_skips(self):
        from declaro_persistum.migrations import partition_replica_operations

        operations = [{"op": "add_column", "table": "child"}]
        safe_order, skipped = partition_replica_operations(operations, [0])
        assert safe_order == [0]
        assert skipped == []
