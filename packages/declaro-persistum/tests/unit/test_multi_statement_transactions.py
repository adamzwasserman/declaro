"""Several ORM writes must be able to commit as one transaction.

pool.transaction() was a no-op passthrough that yielded the pool itself.
Every ORM write took its own acquire_write, so two update_one calls were
two transactions however they were nested. Batching was only possible by
dropping to acquire_write and raw SQL, which defeats the point of having
an ORM.

That gap has a cost beyond tidiness. A caller who wants a row and its
derived index to move together has to choose between raw SQL and doing
them separately — and separately means a failure between the two leaves
the index disagreeing with the row it describes.

Inside transaction(), ORM writes share one connection and one commit:
everything lands or nothing does.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool








# Writes are stateless by default: each one opens its own connection and
# closes it, so there is no single connection left to inspect afterwards.
# These read the whole population instead, which is what the assertions
# always meant — "one commit happened", not "one commit happened on the
# connection the pool happens to keep".


def _commits() -> int:
    return sum(c.commits for c in _Holder.conns)


def _rollbacks() -> int:
    return sum(c.rollbacks for c in _Holder.conns)


def _statements() -> list[str]:
    return [s for c in _Holder.conns for s in c.statements]


async def _pool(tmp_path, monkeypatch, remote=True):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.conns = []
    _Holder.conn_factory = _RecordingConn
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io" if remote else None,
        auth_token="t" if remote else None,
    )
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_replication = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool



