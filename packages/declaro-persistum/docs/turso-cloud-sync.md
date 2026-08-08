# Turso Cloud Sync: Embedded Replica Limitations and Workarounds

## Architecture

declaro-persistum uses pyturso's **embedded replica** mode for Turso Cloud databases:

- A local SQLite file acts as a read replica
- Writes commit locally (sub-ms), then push to Turso Cloud in the background
- On startup, the local replica pulls from cloud to get the latest state

This gives sub-ms read latency while keeping Turso Cloud as the source of truth.

## The Problem: DDL Cannot Be Synced

pyturso's sync engine uses WAL-based replication. It can replicate **DML** (INSERT, UPDATE, DELETE) but **cannot replicate DDL** (CREATE TABLE, ALTER TABLE, DROP TABLE).

When `apply_migrations_async()` creates tables locally and tries to push:

```
sync engine operation failed: database sync engine error:
failed to execute sql: Error { message: "SQLite error: no such table: users" }
```

The push fails because the cloud DB has no schema. The sync engine tries to replay changes against tables that don't exist on cloud.

### Consequences

1. **Tables exist locally but not on cloud** -- reads work on the current instance, but other instances (or restarts) pull empty state from cloud
2. **Data loss on restart** -- `turso.aio.sync.connect()` may pull from cloud on connect, overwriting locally-created tables with empty cloud state
3. **Cross-service invisibility** -- admin and public services each have their own local replica; without working push, they can't share data

## Workaround: `declaro migrate-remote`

A CLI command that creates/updates the schema directly on Turso Cloud, bypassing the embedded replica sync engine entirely.

### First-time setup (empty cloud DB)

```bash
uv run declaro migrate-remote \
  --init \
  --remote "libsql://your-db.turso.io" \
  --token "$TURSO_AUTH_TOKEN" \
  --schema path/to/your_schema.py \
  -v
```

The `--init` flag is **required** when the cloud DB is empty. Without it, the command aborts to prevent accidental data loss (see Safety section below).

### Schema updates (adding columns, altering tables)

```bash
uv run declaro migrate-remote \
  --remote "libsql://your-db.turso.io" \
  --token "$TURSO_AUTH_TOKEN" \
  --schema path/to/your_schema.py \
  -v
```

No `--init` needed -- the command pulls the current cloud schema, diffs against your Pydantic models, and applies only the differences.

### Preview changes without applying

```bash
uv run declaro migrate-remote \
  --dry-run \
  --remote "libsql://your-db.turso.io" \
  --token "$TURSO_AUTH_TOKEN" \
  --schema path/to/your_schema.py
```

### How it works

1. Creates a temporary local file
2. Opens a sync connection (`turso.aio.sync.connect`) to that temp file with the cloud URL
3. Pulls current cloud state into the temp file
4. Introspects the temp file to get current schema
5. Diffs against the target schema (Pydantic models)
6. Applies DDL to the temp file
7. Pushes the changes to cloud
8. Temp file is cleaned up automatically

### After running migrate-remote

Once cloud has the schema:

- App startup: `_initialize()` pulls schema from cloud into the local replica
- Reads: sub-ms from local replica (tables now exist locally)
- Writes: commit locally, push DML to cloud (cloud has matching schema, push succeeds)
- `apply_migrations_async()` at startup finds 0 diff (schema matches) and skips

### When to run it

- **Once per cloud database** for initial schema creation
- **Each time the schema changes** (new columns, altered columns, new tables)
- Can be run from any machine with network access to Turso Cloud -- doesn't need to be on the deployment server

## Safety: Data Loss Prevention

The `--init` flag exists because of a critical edge case:

If the pull from cloud fails silently (the temp file stays empty), the differ sees 0 existing tables and generates `create_table` operations for every table in the schema. When pushed to cloud, this **drops and recreates all tables**, destroying all data.

Without `--init`:
- If 0 tables are found and the diff wants to create tables, the command **aborts** with an explanation
- This is the safe default -- it assumes the pull failed rather than assuming the cloud is genuinely empty

With `--init`:
- The command proceeds to create tables on an empty cloud DB
- Only use this for first-time setup when you know the cloud DB is empty

## Turso Inspector Exclusions

The Turso inspector excludes these system tables from introspection (so the differ doesn't try to drop them):

| Pattern | Tables |
|---------|--------|
| `sqlite_%` | SQLite internal tables |
| `_litestream_%` | Litestream replication tables |
| `_declaro_%` | declaro-persistum metadata (`_declaro_meta`, `_declaro_tmp_*`) |
| `__turso_%` | Turso MVCC metadata (`__turso_internal_mvcc_meta`) |
| `turso_%` | Turso CDC tables (`turso_cdc`, `turso_cdc_version`, `turso_sync_last_change_id`) |

## Replica files on disk — never delete a subset

A cloud replica named `<name>` is **four files**, and they are one atomic unit:

| File | Contents |
|------|----------|
| `<name>` | The main database |
| `<name>-changes` | Change tracking |
| `<name>-info` | `DatabaseMetadata` — the sync configuration and the replica's position in the sync stream |
| `<name>-wal` | The write-ahead log |

Both a raw `turso.aio.sync` connection and `ConnectionPool.turso` produce the same four.

**Never delete some of them and keep the rest.** `-info` is what lets the engine place the local database in the sync stream. Sweep it while keeping `<name>` and the next open fails with `sync engine operation failed: database error: Database schema changed` — the engine has a database it cannot relate to the remote.

If you want a clean re-pull, delete **all four together**. That is a genuine cold start and the engine handles it correctly.

This is not a cleanliness matter. It is what makes a warm re-open fast:

| Re-open, all four files present | Time |
|---|---|
| Raw `turso.aio.sync` | ~1.5ms |
| Through `ConnectionPool.turso` | ~5-7ms |

Measured downstream against a real Turso Cloud remote. With `-info` removed, the open errors rather than being slow.

## Known pyturso Sync Engine Limitations

These are limitations in pyturso's embedded replica sync engine, not declaro-persistum bugs. They may be resolved in future pyturso releases.

**Several entries below were measured and found false in August 2026. They are struck rather than deleted, because declaro-persistum's current design depends on the corrected behaviour, and someone reading the old claims would conclude that design is impossible.**

1. **DDL not replicable** -- CREATE TABLE, ALTER TABLE, DROP TABLE cannot be pushed via the sync engine
2. **Push fails if cloud schema doesn't match** -- DML push requires cloud to already have the target tables
3. **Pull may overwrite local on connect** -- `turso.aio.sync.connect()` may sync from cloud automatically, overwriting locally-committed changes that haven't been pushed. declaro-persistum guards this by pushing before every pull (W3).
4. **Connection cache not refreshed by pull** -- after `pull()`, existing connection objects may not see new tables without close/reopen. This is why `refresh_connections()` exists.
5. ~~**Sync and plain drivers don't share WAL**~~ -- **FALSE, measured 2026-08-06.** A plain `turso.aio.connect()` reader on the same local replica file *does* observe commits made by a `turso.aio.sync` writer. Verified under free-threaded CPython with the GIL confirmed off, with readers tracking a writer to the exact last id under concurrency. declaro-persistum's read path depends on this: reads take plain non-sync connections precisely so they hold no sync state.
6. ~~**Per-connection change tracking**~~ -- **FALSE, measured 2026-08-06.** One sync connection *can* push frames committed by another on the same replica. Verified with 1353 rows written on connection A, 40 pushes issued on connection B, and a fresh third connection pulling all 1353 back from cloud. The dedicated push connection depends on this.
7. ~~**CDC incompatible with MVCC**~~ -- **Not a crash, measured 2026-08-06.** `PRAGMA journal_mode = 'mvcc'` on a cloud replica does not crash. An A/B on a real remote measured MVCC on versus off at 1181ms versus 1030ms per commit, so MVCC is neither fatal nor free. MVCC is requested on every pool by default; pass `mvcc=False` to force WAL.
8. **PRAGMA foreign_keys inside transaction** -- setting this inside a BEGIN may implicitly commit the transaction, breaking atomicity

Entries 5, 6 and 7 were each written as engine facts and each turned out to be a snapshot of an older engine, or of an untested assumption. Verify against the current engine before relying on any entry here.
