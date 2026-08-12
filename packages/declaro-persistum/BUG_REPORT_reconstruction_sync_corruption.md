# Bug: Table reconstruction via embedded replica corrupts both local and cloud DBs

## Severity: CRITICAL — Tables silently destroyed, migration reports success

## Summary

When `apply_migrations_async` detects `alter_column` differences on a Turso embedded replica, it reconstructs the table (create temp, copy data, drop original, rename temp). The DROP syncs to cloud but the CREATE/RENAME does not. After the push failure, the connection is "refreshed" from cloud, which now has the table dropped. Result: table is gone from both local and cloud, but the migration reports success.

## Reproduction

1. Create tables on Turso Cloud via `migrate-remote --init` (creates basic tables without NOT NULL constraints, indexes, or foreign keys)
2. Start an app that uses `ConnectionPool.turso()` with auto-migration enabled
3. Auto-migration detects `alter_column` differences (e.g., NOT NULL constraints missing from `--init` tables)
4. Migration reconstructs tables locally (drop + recreate with correct schema)
5. Push to cloud fails: `sync engine operation failed: database sync engine error: failed to execute sql: Error { message: "SQLite error: no such table: main.<table_name>", code: "SQLITE_UNKNOWN" }`

### Expected

Either:
- Migration detects that push will fail and skips destructive reconstruction, OR
- Reconstruction is atomic (drop + create sync together or not at all), OR
- Migration reports failure when push fails after reconstruction

### Actual

```
WARNING:declaro_persistum.pool:push after acquire_write commit failed
  turso.lib.DatabaseError: sync engine operation failed: ...no such table: subscription_plans
INFO:declaro_persistum.pool:Write holder connection refreshed after migration
INFO:declaro_persistum.migrations:Successfully applied 21 migrations
```

Migration reports "Successfully applied 21 migrations" but the tables no longer exist. Subsequent queries fail with `Parse error: no such table: users`.

The cloud DB also loses the tables — the DROP was synced but the CREATE was not.

## Root cause

Table reconstruction involves these steps:
1. `CREATE TABLE _declaro_tmp_<name>_<hash>` (new schema)
2. `INSERT INTO _declaro_tmp ... SELECT FROM <original>`
3. `DROP TABLE <original>`
4. `ALTER TABLE _declaro_tmp ... RENAME TO <original>`
5. `CREATE INDEX ...`

The Turso sync engine pushes step 3 (DROP) to cloud but fails on subsequent steps. The `push after acquire_write commit failed` error triggers a connection refresh, which pulls the now-dropped state from cloud, destroying the local table too.

## Compounding factor

This creates a vicious cycle:
1. `migrate-remote --init` creates basic tables in cloud
2. App migration sees 28 differences, reconstructs tables, push fails → tables destroyed
3. Next `migrate-remote --init` recreates them → app destroys them again on startup

## Environment

- declaro-persistum: installed via uv from GitHub
- pyturso (turso SDK)
- Turso Cloud embedded replica

## Suggested fixes (pick any)

1. **Wrap reconstruction in a savepoint** and roll back if push fails
2. **Don't push DDL through sync engine** — use direct HTTP for schema changes
3. **Detect embedded replica mode** and skip alter_column operations (log warning instead)
4. **Report failure accurately** — if push fails after DROP, don't report "Successfully applied"
5. **Make `migrate-remote --init` create full schema** (with NOT NULL, FKs, indexes) so embedded replicas see 0 differences
