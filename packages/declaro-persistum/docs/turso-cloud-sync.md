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

## Known pyturso Sync Engine Limitations

These are limitations in pyturso's embedded replica sync engine, not declaro-persistum bugs. They may be resolved in future pyturso releases.

1. **DDL not replicable** -- CREATE TABLE, ALTER TABLE, DROP TABLE cannot be pushed via the sync engine
2. **Push fails if cloud schema doesn't match** -- DML push requires cloud to already have the target tables
3. **Pull may overwrite local on connect** -- `turso.aio.sync.connect()` may sync from cloud automatically, overwriting locally-committed changes that haven't been pushed
4. **Connection cache not refreshed by pull** -- after `pull()`, existing connection objects may not see new tables without close/reopen
5. **Sync and plain drivers don't share WAL** -- `turso.aio.sync.connect()` and `turso.aio.connect()` on the same file don't see each other's writes
6. **Per-connection change tracking** -- each sync connection tracks its own changes; one connection can't push changes committed by another
7. **CDC incompatible with MVCC** -- `PRAGMA journal_mode = 'mvcc'` crashes when cloud sync (CDC replication) is active
8. **PRAGMA foreign_keys inside transaction** -- setting this inside a BEGIN may implicitly commit the transaction, breaking atomicity
