"""Concurrent writers must actually run concurrently.

Turso supports concurrent writes through BEGIN CONCURRENT over MVCC, and the
pool asks for MVCC on every pool. That achieved nothing on its own: every
writer went through one shared connection under one lock, so callers were
serialized before they ever reached the connection. BEGIN CONCURRENT was
issued into a queue of one.

Each writer now takes its own replica connection, so max_size bounds how many
writers proceed rather than how many queue.

A push on a separate connection delivers the frames committed on a write
connection — verified downstream under free-threaded CPython with the GIL
off. These tests cover the pool's side: that writers overlap, that each gets
its own connection, and that the connections are returned and reused rather
than leaked.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

WRITE_WORK = 0.10
WRITERS = 5








async def _pool(
    tmp_path, monkeypatch, max_size=WRITERS, remote=True,
):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.instances = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io" if remote else None,
        auth_token="t" if remote else None,
        max_size=max_size,
    )
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool






