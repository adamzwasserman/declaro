"""Writers must actually run concurrently where the engine allows it.

Turso supports concurrent writers through MVCC and `BEGIN CONCURRENT`.
Conflicts are detected at commit, at row granularity, so writers touching
different rows do not interfere. `Error::Busy`, `Error::BusySnapshot` and
errors containing "conflict" are documented and retryable — the prescribed
handling is to roll back and retry.

    https://docs.turso.tech/tursodb/concurrent-writes

MVCC RUNS ONLY ON LOCAL POOLS BY PERSISTUM'S CHOICE, not the engine's, so
"where the engine allows it" means a pool with no `remote_url`. The engine
will run MVCC on a replica quite happily; what it will not do is take
a second replica connection to one replica. These tests used a cloud pool and then forced `_mvcc = True`
onto it, which asserted a configuration persistum now refuses to create:
MVCC on a replica strands writes (declaro-p39). The overlap tests
therefore build a LOCAL pool. Only the push test, which needs a remote at
all, stays replicated — and it does not need MVCC to make its point.

0.1.22 added a blanket lock that serialised every writer on a cloud replica,
on the reading that two writers lose a write. That serialised away the
feature the engine exists to provide: the connections were opened per caller
and then queued one at a time behind the lock. Conflicts are retried now
instead of prevented.

The push does not take a lock either, so it blocks nobody.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool








async def _pool(tmp_path, monkeypatch, *, replicated: bool):
    """`replicated` is required. It is the ONLY thing that decides the engine mode.

    No default, because a default would let a test silently pick an arm and
    give no reader a way to tell "chose local" from "did not think about it" —
    and picking the wrong arm here is exactly the defect under test.
    """
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

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
    assert pool._mvcc is (not replicated), (
        "the engine choice did not follow remote_url — that rule is the whole "
        "safety property, so a test built on a pool that broke it proves nothing"
    )
    return pool






