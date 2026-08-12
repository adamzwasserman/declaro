# Bug Report: CDC/MVCC conflict breaks cloud sync

## Summary

`TursoPool._initialize()` unconditionally sets `PRAGMA journal_mode = 'mvcc'`, which crashes when cloud sync (CDC replication) is active.

## Error

```
sync engine operation failed: database error: Parse error: CDC is not supported in MVCC mode
```

## Reproduction

```python
pool = await ConnectionPool.turso(
    "./data/central.db",
    remote_url="libsql://mc-central-xxx.turso.io",
    auth_token="eyJ...",
)
# Crashes during _initialize() because:
# 1. connect_async() opens with turso.aio.sync.connect() (CDC mode)
# 2. _initialize() runs PRAGMA journal_mode = 'mvcc'
# 3. MVCC conflicts with CDC → exception
# 4. The exception handler catches it, but the commit() on line 601
#    triggers the sync engine which fails
```

## Root Cause

In `pool.py` `TursoPool._initialize()` (line ~584):

```python
cur = await self._write_holder.conn.execute("PRAGMA journal_mode = 'mvcc'")
```

This runs regardless of whether `remote_url` is set. When cloud sync is active, the connection uses CDC replication, which is incompatible with MVCC journal mode.

## Suggested Fix

Skip the MVCC pragma when `self._remote_url` is set. CDC replication has its own journaling; MVCC is only useful for local-only connections.

Additionally, the `PRAGMA cache_size` and subsequent `commit()` may also interact badly with CDC — the maintainer should verify.

## Environment

- pyturso 0.5.1 (PyPI, pre-built wheels)
- Also reproduced with pyturso 0.6.0rc1 (git source)
- Turso Cloud URL format: `libsql://...turso.io`
- Deployed on Render (Linux x86_64)

## Impact

Blocks all cloud sync usage. Admin, public, and prod services cannot share a central Turso database.
