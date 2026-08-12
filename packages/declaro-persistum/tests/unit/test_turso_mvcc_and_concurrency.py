"""Tests for MVCC on cloud pools, and for read concurrency.

Turso supports concurrent writes with BEGIN CONCURRENT over MVCC. The pool
enabled MVCC only when there was no remote_url, so every cloud pool ran
serialized writes while the engine below it supported concurrent ones.

The pool also served every read from one shared connection under one lock,
and held that lock for as long as the caller held the connection. max_size
therefore bounded how many callers could queue, not how many could proceed:
five concurrent readers doing 100ms of work each took 505ms.

Both defects are measured here, not argued.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

# Work time per reader. Long enough to separate parallel from serial with a
# wide margin, short enough to keep the suite fast.
READ_WORK = 0.10
READERS = 5








async def _init(pool, conn):
    """Run _initialize with the holder and background work stubbed out."""
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    orig = pool_mod._TursoConnectionHolder
    pool_mod._TursoConnectionHolder = lambda *_a, **_kw: _Holder(conn)  # type: ignore[assignment]
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    try:
        await pool._initialize()
    finally:
        pool_mod._TursoConnectionHolder = orig  # type: ignore[assignment]






async def _seed_real_db(path: str) -> None:
    """Create a genuine local database, so read connections can open it."""
    import turso.aio

    conn = await turso.aio.connect(path)
    await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    await conn.execute("INSERT INTO t VALUES (1, 'seed')")
    await conn.commit()
    await conn.close()


