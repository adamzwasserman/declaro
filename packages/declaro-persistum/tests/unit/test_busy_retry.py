"""A busy database must not reach the caller as an error.

Concurrent writes to one cloud replica can fail with:

    replication engine operation failed: database tape error: database is busy

Measured downstream: 30 concurrent writes across 8 replicas produced 5
failures, surfaced to users as HTTP 500. A single writer never sees it; it
appears only when two writes land on one replica at once.

This is I/O nastiness reaching the consumer, which is the one thing this
pool exists to prevent. A busy database means "not now", not "no". The
pool absorbs it and retries.

Retries happen at transaction boundaries only — starting the transaction,
and committing it. Both are points where the safety argument is clear:

  - BEGIN failed, so nothing was staged and nothing applied
  - commit failed, so the transaction did not land; the staged statements
    are still there and re-committing lands the same set

A statement that fails mid-transaction is NOT retried here. The pool
cannot replay the caller's statements, and a half-applied transaction is
not something to guess about, so it propagates.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

BUSY = "replication engine operation failed: database tape error: database is busy"








def _pool(tmp_path, monkeypatch, conn, **kw):
    """A pool whose every write connection is `conn`.

    Writes are stateless by default, so each one OPENS a connection rather
    than reusing the pool's. Patching `pool._write_holder` alone no longer
    reaches the write path — it would let the real driver dial the fake
    remote. The module-level holder is patched instead, and every holder
    hands back the same `conn` so the test can count begins and commits
    across the whole write.
    """
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py


    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _SharedHolder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db), remote_url="https://example.turso.io", auth_token="t", **kw
    )
    pool._write_holder = _Holder(conn)  # type: ignore[assignment]
    pool._mvcc = True
    return pool



