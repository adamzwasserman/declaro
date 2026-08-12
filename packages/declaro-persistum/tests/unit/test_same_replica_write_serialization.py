"""Writers to one replica: concurrent under MVCC, serialised without it.

Measured against a real Turso Cloud replica, concurrent writers to one
replica, distinct rows so there is no logical conflict:

                    K=2    K=5    K=10   K=20
    MVCC on         2/2    4/5    9/10   20/20     (with no lock at all)
    MVCC off        1/2    3/5    3/10   6/20

With MVCC, twenty concurrent writers land twenty writes and a lock changes
nothing. Without it, single-writer is Turso's documented default and the
second writer is rejected from inside the write statement — so a lock is the
only thing that keeps those writes.

0.1.22 added that lock unconditionally, on a measurement taken in WAL mode.
That measurement is the MVCC-off row above: real, but a measurement of the
engine running without the feature rather than a limit of the engine. The
serialisation is now conditional on MVCC being off.

    https://docs.turso.tech/tursodb/concurrent-writes

The fake below models exactly that: it rejects a second concurrent writer
only when it is not in MVCC mode.

WHICH ARM A POOL GETS IS NO LONGER A TEST'S CHOICE. persistum runs MVCC on
local pools only, so the two arms here are two POOL SHAPES, not two settings
on one shape:

    MVCC arm  ->  local pool, no remote_url
    WAL arm   ->  replicated pool, remote_url set

These tests previously built a replicated pool for both arms and passed
`mvcc=True` to the constructor. That parameter is gone, and the
configuration it selected is the one persistum exists to make unreachable.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool








async def _pool(tmp_path, monkeypatch, *, mvcc: bool, name="r.db"):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _TapeConn.active_writers = {}
    _TapeConn.collisions = 0
    _TapeConn.mvcc = mvcc
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / name
    db.write_bytes(b"x" * 64)
    # MVCC iff local. The arm IS the pool shape; nothing else selects it.
    remote = (
        {} if mvcc else {"remote_url": "https://example.turso.io", "auth_token": "t"}
    )
    pool = TursoPool(str(db), max_size=24, **remote)
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_replication = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    assert pool._mvcc is mvcc, (
        "the pool did not land in the arm this test needs; the engine choice "
        "follows remote_url and nothing else"
    )
    return pool


async def _write_all(pool, k):
    errors = []

    async def writer(n):
        try:
            async with pool.acquire_write() as conn:
                await conn.execute(f"INSERT INTO t VALUES ({n})")
        except Exception as e:  # noqa: BLE001 - collected, then asserted on
            errors.append(str(e))

    await asyncio.gather(*(writer(n) for n in range(k)))
    return errors






