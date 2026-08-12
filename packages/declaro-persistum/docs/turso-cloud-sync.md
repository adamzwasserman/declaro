# Turso Cloud Sync: Embedded Replica Limitations and Workarounds


> ## ⚠️ DEPRECATED — POISONOUS PRACTICE
>
> **Every example below that hands a `ConnectionPool` to the consumer is poisonous practice. Do not copy it, and do not write new code in this shape.**
>
> The pool decision must never reach the consumer or the common syntax. The consumer chooses **async (default) or sync**, and nothing else. Whether a pool exists behind that choice, whether a write reuses a connection, and whether the engine runs MVCC or WAL are all internal, owned by exactly one writer, and invisible above that boundary.
>
> A pool exposed as a surface is promiscuous mutable state with no determinable owner — measured directly on this codebase 2026-08-11, where L1.18b reported the pool's holder fields as `unresolved, drives a decision`. It is the reason every conditional about MVCC kept ending up inside the pool: with no single owner, a branch had no outside to live in.
>
> Binding constraint: **[docs/design/state-ownership-and-the-pool-boundary.md](design/state-ownership-and-the-pool-boundary.md)**
>
> This document is retained as a record. It is not guidance.

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

## How many concurrent writers one replica sustains

**It depends entirely on whether MVCC is on. With it, twenty. Without it, one.**

Measured against a real Turso Cloud replica, concurrent writers to one replica, distinct rows so there is no logical conflict:

| | K=2 | K=5 | K=10 | K=20 |
|---|---|---|---|---|
| **MVCC on**, no serialisation at all | 2/2 | 4/5 | 9/10 | 20/20 |
| **MVCC off** (WAL) | 1/2 | 3/5 | 3/10 | 6/20 |

Twenty concurrent writers land twenty writes with no lock anywhere. Single-writer is Turso's **documented default**, and MVCC is what lifts it — see [Concurrent Writes](https://docs.turso.tech/tursodb/concurrent-writes) and [Beyond the Single-Writer Limitation](https://turso.tech/blog/beyond-the-single-writer-limitation-with-tursos-concurrent-writes). MVCC is requested on every pool by default; pass `mvcc=False` to force WAL, and the pool then serialises writers so none are lost.

### Correction

This section previously reported a "2–3 concurrent writer ceiling" and called it the engine at its limit, with advice to shard around it. **That was wrong, and the advice was wrong.**

The original measurement is real — it is the MVCC-off row above. But it was taken with the engine running in WAL mode, where rejecting the second writer is documented default behaviour. It measured the absence of a feature, not a limit. The explanation later attached to it here — that the sync engine is a single-appender log — was invented and is not in any Turso documentation.

If you were told to shard hot data because of this ceiling, that advice does not apply. Check that MVCC is active first: the pool logs it at startup, and `PRAGMA journal_mode` returns `mvcc` when it is.

The residual failures under MVCC — one at K=5, one at K=10 — are the background push contending with writers, not writers contending with each other. The push absorbs that by retrying, which is safe because it ships frames rather than replaying a caller's statements.

## Known pyturso Sync Engine Limitations

**This list has been emptied. Every entry that was here was written as an engine fact and stated without a measurement; three were later disproved outright, and the rest were never checked.**

Nothing is recorded here unless it carries a date and the measurement that produced it. If you need to know whether the engine does something, measure it against a throwaway database and write the number down. Do not restore a claim from memory or from an older revision of this file.

Measured facts currently held about this engine live in the sections above, each with its date and its numbers.

### Open, cause unknown

**Writes are stranded under MVCC with concurrent write connections.** Measured 2026-08-10 against a real cloud replica: 17 writes reported ok, 15 locally durable, 4 reached the primary, and it did not converge in 348 seconds — plateauing at 4 after the first minute. Making the push cover every write connection did not change it.

Separately measured the same day: concurrent writers to one table under MVCC raise `turso.Error: Write-write conflict` even for distinct rows. That error is documented as retryable, and `acquire_write` does not retry it — it cannot replay a caller's statements. Whether the conflicts and the stranding are the same failure is **not established**.
