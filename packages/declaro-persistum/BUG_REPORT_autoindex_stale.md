# Bug: Stale `sqlite_autoindex_*_new_1` indexes block table reconstruction

## Summary

Table reconstruction fails with `Parse error: index "sqlite_autoindex_{table}_new_1" already exists` when a second `alter_column` op targets the same table in a single migration batch. The error also persists across migration runs if a previous run left stale autoindexes.

## Reproduction

Schema diff emits two `alter_column` ops for the same table (e.g., `database_routes`). The applier runs them sequentially:

1. First `alter_column` → reconstruction succeeds → `sqlite_autoindex_database_routes_new_1` gets created by SQLite for the temp table, then survives the `ALTER TABLE ... RENAME TO` as a stale artifact
2. Second `alter_column` → reconstruction fails because `sqlite_autoindex_database_routes_new_1` already exists

## Observed logs (Render production)

```
INFO:declaro_persistum.abstractions.reconstruction:Successfully reconstructed table 'database_routes'
INFO:declaro_persistum.applier.turso:Applied: alter_column on database_routes
ERROR:declaro_persistum.abstractions.reconstruction:Table reconstruction failed for 'database_routes': Parse error: index "sqlite_autoindex_database_routes_new_1" already exists
WARNING:declaro_persistum.applier.turso:Skipped unsupported operation: alter_column on database_routes: Parse error: index "sqlite_autoindex_database_routes_new_1" already exists
```

Same pattern for `pricing_invites`, `users`, `subscriptions`, `billing_events`.

## Analysis

The reconstruction code in `reconstruction.py` now uses UUID-based temp names (`_declaro_tmp_{table}_{uuid}`), which should prevent this. However:

1. **Legacy stale indexes**: Previous versions may have used `{table}_new` naming. If those indexes survive in the database, any new reconstruction attempt that creates a temp table will collide if SQLite internally reuses the `_new` suffix pattern.

2. **SQLite autoindex naming**: When SQLite creates a table with UNIQUE or PK constraints, it auto-creates indexes named `sqlite_autoindex_{table}_N`. After `ALTER TABLE temp RENAME TO real`, these autoindexes keep their original name. So a table created as `_declaro_tmp_database_routes_abc123` would get `sqlite_autoindex__declaro_tmp_database_routes_abc123_1`, which is fine. But if there's a stale `sqlite_autoindex_database_routes_new_1` from a legacy `database_routes_new` temp table, it collides.

3. **`_recover_orphaned_tmp_tables`** (migrations.py line 304) cleans up `_declaro_tmp_*` tables but may not clean up legacy `*_new` tables or their stale autoindexes.

## Suggested fixes

### Fix A: Drop stale autoindexes before reconstruction

In `execute_reconstruction_async()`, before creating the temp table, query `sqlite_master` for any `sqlite_autoindex_*_new_*` indexes referencing the target table and drop them:

```python
# Before creating temp table, clean up stale autoindexes from legacy naming
cursor = await connection.execute(
    "SELECT name FROM sqlite_master WHERE type='index' "
    "AND name LIKE 'sqlite_autoindex_%_new_%'"
)
stale = await cursor.fetchall()
for (idx_name,) in stale:
    await connection.execute(f'DROP INDEX IF EXISTS "{idx_name}"')
```

### Fix B: Coalesce same-table operations in the applier

If the differ emits N `alter_column` ops for the same table, the applier should merge them into a single reconstruction pass. This is both safer (one reconstruction instead of N) and faster.

In `TursoApplier.apply()`, group operations by table before the execution loop:

```python
# Group reconstruction ops by table, apply as single reconstruction
from itertools import groupby
# ... merge alter_column ops targeting same table into one combined op
```

### Fix C: Expand orphan recovery to handle legacy `_new` tables

In `_recover_orphaned_tmp_tables`, also look for `{table}_new` pattern tables and drop them along with their autoindexes.

## Affected versions

Observed in production with declaro_persistum installed from pip (version in multicardz .venv). The UUID temp naming was added to prevent this, but legacy databases still have stale artifacts.

## Impact

- Non-fatal: the second `alter_column` is skipped, and the migration reports partial success
- But the skipped operations mean schema drift accumulates — the same ops re-run on every deploy
- Combined with the migration hash not being pushed to cloud (separate issue, fixed in multicardz), this caused 31 migration ops on every single deploy
