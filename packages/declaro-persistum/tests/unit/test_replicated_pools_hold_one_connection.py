"""A replicated pool holds ONE write connection. A local pool opens one per write.

REGRESSION TEST for declaro-dna, a defect I shipped in 0.3.0.

0.1.28 pooled write connections and returned them to a free list, so a pool
held at most `max_size` of them. 0.3.0 opened one per write and closed it,
unconditionally — no branch on whether the pool was replicated.

Every pyturso connection is its own OS worker thread, and on a REPLICATED pool
every open is a `turso.aio.sync` handshake against the replica tape. So the
change turned a burst that cost ~5 held connections into one open, one thread
and one tape acquisition per write. multicardz seeded 200 signups on 0.1.x and
died at 20 concurrent on 0.3.0 with SIGKILL.

It also contradicted this package's own measurement from the same day
(declaro-eer): a replica takes ONE replica connection, and opening a
second returns "database tape error: database is busy" — 3 of 4 probe runs
failed outright. 0.1.28 pooled and never hit it. Removing the pooling
reintroduced exactly the pattern the probe says fails.

THE SCOPE ERROR, recorded because it is the reusable lesson: the instruction
was to make writes stateless "for turso embedded". Turso embedded is the LOCAL
case. Stateless is right there — no tape, and an open measured at 1.16ms — and
wrong on a replicated pool, which 0.1.28 knew and I discarded.

The vocabulary is two members, replicated and local, so both run here rather than
one plus an assumption about the other.
"""

import asyncio

import pytest

from declaro_persistum.turso_pool import TursoPool


class _Cursor:
    def __init__(self, rows):
        self._rows = rows
        self.description = None
        self.rowcount = 1

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self):
        self.closed = False

    async def execute(self, sql, *_a):
        if "journal_mode" in sql and "mvcc" in sql:
            return _Cursor([("mvcc",)])
        return _Cursor([])

    async def commit(self):
        pass

    async def rollback(self):
        pass

    async def close(self):
        self.closed = True


class _Holder:
    """Counts opens, which is the quantity the regression changed."""

    opened: list["_Holder"] = []

    def __init__(self, database_path, remote_url=None, _auth_token=None):
        self.database_path = database_path
        self._remote_url = remote_url
        self.conn = None
        type(self).opened.append(self)

    async def connect_async(self):
        self.conn = _Conn()

    async def push(self):
        pass

    async def pull(self):
        pass


async def _pool(tmp_path, monkeypatch, *, replicated: bool):
    import declaro_persistum.turso_pool as pool_mod

    _Holder.opened = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)
    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    remote = (
        {"remote_url": "https://example.turso.io", "auth_token": "t"} if replicated else {}
    )
    pool = TursoPool(str(db), **remote)
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_replication = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    _Holder.opened = []  # count only what the WRITES open
    return pool


WRITES = 20


@pytest.mark.asyncio
async def test_a_replicated_pool_opens_no_connection_per_write(tmp_path, monkeypatch):
    """The regression, stated as the count that changed.

    Twenty writes must not cost twenty replication-engine handshakes against the
    replica tape.
    """
    pool = await _pool(tmp_path, monkeypatch, replicated=True)

    for _ in range(WRITES):
        async with pool.acquire_write() as conn:
            await conn.execute("INSERT INTO t VALUES (1)")

    assert len(_Holder.opened) == 0, (
        f"{len(_Holder.opened)} new replica connections for {WRITES} writes on a "
        f"REPLICATED pool; a replica takes one (declaro-eer), and opening "
        f"per write is what killed multicardz's stage box (declaro-dna)"
    )


@pytest.mark.asyncio
async def test_a_replicated_pool_reuses_the_held_connection(tmp_path, monkeypatch):
    """Not merely 'few opens' — the SAME connection, every time."""
    pool = await _pool(tmp_path, monkeypatch, replicated=True)

    seen = []
    for _ in range(WRITES):
        async with pool.acquire_write() as conn:
            seen.append(id(conn._holder))

    assert len(set(seen)) == 1, f"{len(set(seen))} distinct write connections"


@pytest.mark.asyncio
async def test_a_replicated_pool_does_not_close_its_connection_after_a_write(
    tmp_path, monkeypatch
):
    """Held means held. A closed connection is reopened on the next write."""
    pool = await _pool(tmp_path, monkeypatch, replicated=True)

    async with pool.acquire_write() as conn:
        await conn.execute("INSERT INTO t VALUES (1)")
        held = conn._holder

    assert held.conn is not None, "the replicated pool closed its write connection"
    assert held.conn.closed is False


@pytest.mark.asyncio
async def test_a_local_pool_still_opens_one_per_write(tmp_path, monkeypatch):
    """The other member. Stateless is correct where there is no tape.

    Asserted so a fix for the replicated case cannot quietly make every pool
    stateful — the cost ledger in `_write_connection` argues against that,
    and it would be an unmeasured change on the local path.
    """
    pool = await _pool(tmp_path, monkeypatch, replicated=False)

    for _ in range(WRITES):
        async with pool.acquire_write() as conn:
            await conn.execute("INSERT INTO t VALUES (1)")

    assert len(_Holder.opened) == WRITES, (
        f"a LOCAL pool opened {len(_Holder.opened)} connections for {WRITES} "
        f"writes; stateless is correct here and measured cheap (1.16ms)"
    )
    assert all(h.conn is None for h in _Holder.opened), (
        "a stateless write left its connection open"
    )


@pytest.mark.asyncio
async def test_concurrent_writers_on_a_replicated_pool_open_nothing(
    tmp_path, monkeypatch
):
    """The burst shape that crashed: many writers at once, still no opens.

    The replica lock serialises them, so they share the held connection
    rather than each opening a tape.
    """
    pool = await _pool(tmp_path, monkeypatch, replicated=True)

    async def writer():
        async with pool.acquire_write() as conn:
            await conn.execute("INSERT INTO t VALUES (1)")

    await asyncio.gather(*(writer() for _ in range(WRITES)))

    assert len(_Holder.opened) == 0, (
        f"{len(_Holder.opened)} connections opened by {WRITES} concurrent "
        f"writers on a replicated pool"
    )
