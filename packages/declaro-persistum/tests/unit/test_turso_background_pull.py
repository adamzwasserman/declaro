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













