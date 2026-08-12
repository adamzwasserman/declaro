"""The push must not hold a connection of its own.

Nothing waits on the push -- the write is already durable locally before it
runs -- so it can queue behind writes rather than hold a second connection
open against the replica.

This is NOT a claim that the replication engine takes a single writer. Turso
supports concurrent writers through MVCC and BEGIN CONCURRENT, and the pool
still opens a write connection per concurrent caller. `Error::Busy` at
commit is a documented, retryable conflict signal.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool








async def _pool(tmp_path, monkeypatch):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.opened = []
    _Holder.pushes = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(str(db), remote_url="https://example.turso.io", auth_token="t")
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_replication = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool




