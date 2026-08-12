"""No consumer operation may wait on the pool's own bookkeeping.

The pool exists to keep I/O realities away from callers. A caller should
never wait on a lock, never queue behind a concurrency cap, and never be
refused because the pool ran out of slots.

Three things violated that:

  - _conn_lock, still taken when a push fell back to the write connection
  - a read semaphore of max_size, so reader max_size+1 queued
  - a write semaphore of max_size, so writer max_size+1 queued, and could
    be refused outright with PoolExhaustedError

max_size now bounds how many idle connections are RETAINED, not how many
callers may proceed. Concurrency is unbounded; retention is bounded. A
caller above max_size opens a connection, uses it, and closes it on
release rather than waiting for someone else's.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

WORK = 0.10
MAX_SIZE = 2
CALLERS = 8








async def _pool(tmp_path, monkeypatch, max_size=MAX_SIZE):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.instances = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io",
        auth_token="t",
        max_size=max_size,
    )
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool










