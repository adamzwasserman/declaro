# pyturso Integration: What We Wrap and Why

## Overview

declaro-persistum wraps pyturso's embedded replica connection in `TursoPool` to provide a reliable, application-safe interface. pyturso provides the raw SQLite-compatible database engine with cloud sync. We add the safety, ordering, and lifecycle management that production applications need.

This document explains each wrapping decision and the pyturso behavior that necessitated it.

## Connection Management

### What pyturso provides

```python
# Local-only connection
conn = await turso.aio.connect("local.db")

# Embedded replica with cloud sync
conn = await turso.aio.sync.connect("local.db", remote_url="libsql://...", auth_token="...")
await conn.push()  # send local changes to cloud
await conn.pull()  # fetch cloud changes to local
```

### What we wrap

**Single shared connection** (`_write_holder`). Both reads and writes go through one pyturso connection. pyturso connections are single-threaded internally (worker thread model). Creating separate connections per operation caused three problems:

1. **Sync state isolation** — each pyturso sync connection tracks its own changes. Connection A can't push changes committed by Connection B. Writes on ephemeral connections were committed locally but lost to cloud sync when the connection closed.

2. **Driver mismatch** — `turso.aio.connect()` (plain) and `turso.aio.sync.connect()` (sync) on the same file don't share WAL state. Tables created by one driver are invisible to the other.

3. **Pull-on-connect** — `turso.aio.sync.connect()` may pull from cloud on connect, overwriting locally-committed data that hasn't been pushed yet.

**Connection lock** (`_conn_lock`). An `asyncio.Lock` prevents reads from blocking on cloud push I/O. Without it, the push loop's cloud round-trip (hundreds of ms) blocks reads on the shared connection, inflating sub-ms local reads to 750ms+.

- `acquire()` holds the lock only during the local SQLite read
- `acquire_write()` holds the lock during local commit, releases before push
- `_push_once()` acquires the lock only during the push call

## Cloud Sync

### What pyturso provides

`push()` sends accumulated WAL changes to Turso Cloud. `pull()` fetches cloud changes into the local file. Both are explicit calls — there is no automatic sync.

### What we wrap

**Push loop** (`_push_loop`). A background `asyncio.Task` that pushes every `push_interval_s` seconds (default 1s) with exponential backoff on failure (capped at 30s). Retries indefinitely — never gives up. This is the guaranteed eventual consistency mechanism.

**Non-blocking write path**. `acquire_write()` commits locally and attempts one push (best-effort). If push fails, the push loop picks it up. The caller is never blocked by cloud latency.

**Blocking shutdown**. `close()` retries push indefinitely until cloud confirms receipt. On ephemeral infrastructure (no persistent disk), data not pushed before shutdown is lost permanently. The shutdown push is the last line of defense.

**Push pause** (`pause_push` / `resume_push`). Migrations pause the push loop during DDL to prevent the sync engine from seeing partial state (e.g., a table that's been dropped but not yet renamed during reconstruction).

## DDL (Schema Migrations)

### What pyturso's sync engine cannot do

pyturso's sync engine uses WAL-based replication. It can replicate DML (INSERT, UPDATE, DELETE) but **cannot replicate DDL** (CREATE TABLE, ALTER TABLE, DROP TABLE). When migration creates tables locally and tries to push:

```
sync engine operation failed: failed to execute sql:
Error { message: "SQLite error: no such table: users" }
```

The cloud has no schema. The sync engine tries to replay changes against tables that don't exist.

### What we wrap

**`migrate-remote` CLI command**. Creates a temp local file, connects via `turso.aio.sync.connect()`, pulls current cloud state, diffs against the target schema, applies DDL locally, pushes to cloud. This bypasses the sync engine's DDL limitation by using a dedicated sync connection that starts clean.

```bash
uv run declaro migrate-remote \
  --init \
  --remote "libsql://your-db.turso.io" \
  --token "$TOKEN" \
  --schema schema.py
```

**Reconstruction guard**. `apply_migrations_async()` on an embedded replica skips operations that require table reconstruction (`alter_column`, `add_foreign_key`, `drop_foreign_key`). These operations involve DROP TABLE + CREATE TABLE, and partial sync (DROP reaches cloud, CREATE doesn't) destroys tables on both sides. A warning directs the user to `migrate-remote`.

**Orphaned table recovery**. If a previous reconstruction left `_declaro_tmp_*` or `*_new` tables behind, `apply_migrations_async()` detects and recovers them before running migrations.

## PRAGMA Compatibility

### What pyturso does differently

pyturso supports most SQLite PRAGMAs but not all, and error messages differ from SQLite's.

| PRAGMA | pyturso behavior |
|--------|-----------------|
| `table_info` | Supported |
| `index_list` | Supported |
| `index_info` | Supported |
| `foreign_key_list` | Not supported — emulation needed |
| `foreign_keys = ON/OFF` | Supported, but inside a transaction may implicitly commit |
| `journal_mode = 'mvcc'` | Crashes when CDC replication is active |
| `cache_size` | May not be supported |

### What we wrap

**`pragma_compat` module**. Try-native-first with fallback to emulation. Error matching catches four pyturso error formats: `"not supported"`, `"no such pragma"`, `"unknown"`, `"not a valid"` (pyturso's specific format: `"Parse error: Not a valid pragma name"`).

**MVCC/cache_size guard**. `_initialize()` skips MVCC and cache_size PRAGMAs when `remote_url` is set (CDC replication is incompatible with MVCC journal mode).

**FK PRAGMA outside transactions**. Table reconstruction sets `PRAGMA foreign_keys = OFF` before `BEGIN`, not inside the transaction. Setting it inside may implicitly commit the transaction, breaking atomicity and causing orphaned tables.

## Inspector Exclusions

pyturso and Turso Cloud create internal tables that appear in `sqlite_master`. The differ would generate DROP TABLE operations for these (since they're not in the target schema), which would break the database.

```sql
SELECT name FROM sqlite_master
WHERE type = 'table'
  AND name NOT LIKE 'sqlite_%'
  AND name NOT LIKE '_litestream_%'
  AND name NOT LIKE '_declaro_%'
  AND name NOT LIKE '__turso_%'
  AND name NOT LIKE 'turso_%'
```

| Pattern | Tables |
|---------|--------|
| `sqlite_%` | SQLite internals |
| `_litestream_%` | Litestream replication |
| `_declaro_%` | declaro-persistum metadata and temp reconstruction tables |
| `__turso_%` | `__turso_internal_mvcc_meta` |
| `turso_%` | `turso_cdc`, `turso_cdc_version`, `turso_sync_last_change_id` |

## FK Ordering

### What pyturso's sync engine does wrong

The sync engine replays writes in an arbitrary order. If a parent row and child row (with FK reference) are pushed together, the sync engine may try to insert the child before the parent, causing:

```
SQLite error: FOREIGN KEY constraint failed
```

### What we wrap

**`fk_ordering` module**. Pure functions that topologically sort tables by FK dependencies:

- `fk_insert_order(schema)` — parents first
- `fk_delete_order(schema)` — children first
- `sort_operations(schema, ops)` — sort a DML batch by FK deps
- `execute_fk_ordered(pool, schema, ops)` — batch execution in FK-safe order

**`migrate-remote --no-fks`**. Creates cloud tables without FK constraints. FK enforcement stays on the local replica (where write order is controlled). The cloud becomes a data store without referential integrity enforcement, avoiding sync engine replay-order violations.

**`strip_foreign_keys(schema)`**. Pure function that returns a schema copy with all `references`, `on_delete`, `on_update` removed.

## Table Reconstruction

### What SQLite/pyturso cannot do

SQLite has no `ALTER COLUMN`. Changing column type, nullability, or default requires table reconstruction: create new table, copy data, drop old, rename new.

### What we wrap

**UUID temp table names**. Reconstruction uses `_declaro_tmp_{table}_{uuid8}` as the temp table name. Previous naming (`{table}_new`) caused `sqlite_autoindex` collisions: SQLite doesn't rename autoindexes during `ALTER TABLE RENAME`, so the old autoindex name persists and collides if reconstruction runs again.

**Coalesced reconstruction**. Multiple `alter_column` ops on the same table are merged into a single reconstruction pass: introspect once, apply all changes, reconstruct once. Without this, each op creates and renames a temp table, and autoindexes from the first reconstruction collide with the second.

**DROP TABLE IF EXISTS before CREATE**. Reconstruction drops the temp table name before creating it, clearing any orphaned autoindexes from previous failed runs.

## Summary of Wrapping Layers

```
Application code
    |
    v
TursoPool (pool.py)
    |-- acquire()        read via shared _write_holder, under _conn_lock
    |-- acquire_write()  write via shared _write_holder, push outside lock
    |-- _push_loop()     background eventual consistency, infinite retry
    |-- close()          blocking final push on shutdown
    |-- pause/resume     migration DDL safety
    |
    v
TursoAsyncConnection (pool.py)
    |-- execute()        delegates to _write_holder.conn
    |-- commit()         local commit only
    |
    v
_TursoConnectionHolder (pool.py)
    |-- connect_async()  turso.aio.sync.connect() with remote_url
    |-- push()           WAL sync to cloud
    |-- pull()           cloud sync to local
    |
    v
pyturso (turso.aio.sync)
    |-- raw SQLite-compatible engine
    |-- WAL-based cloud replication
```

Each layer exists because pyturso's raw interface has a specific limitation or safety gap that production applications cannot tolerate.
