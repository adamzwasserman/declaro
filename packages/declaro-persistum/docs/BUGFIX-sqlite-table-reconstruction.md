# SQLite Table Reconstruction Implementation

## Problem Statement

SQLite has significant limitations on ALTER TABLE operations:
- Cannot ADD FOREIGN KEY after table creation
- Cannot DROP FOREIGN KEY
- Cannot DROP COLUMN if column has UNIQUE constraint
- Cannot ALTER COLUMN (type, nullability, default)

The existing declaro_persistum SQLite applier raised `NotImplementedError` for these operations, blocking migrations.

## Root Cause

The multicardz migration was failing with:
```
SQLite doesn't support adding foreign keys after table creation.
Foreign key on 'group_memberships.group_id' requires table reconstruction.
```

## Solution Approach

SQLite's recommended approach for schema changes is **table reconstruction**:
1. Create new table with desired schema
2. Copy data from old table
3. Drop old table
4. Rename new table to original name
5. Recreate indexes

## Changes Made (in vendored copy only)

### 1. `abstractions/table_reconstruction.py`

Added new functions:
- `add_foreign_key(conn, table, column, references, on_delete, on_update)` - Add FK via reconstruction
- `drop_foreign_key(conn, table, column)` - Remove FK via reconstruction
- `drop_column(conn, table, column)` - Drop column via reconstruction (handles UNIQUE)

Updated:
- `_get_full_table_schema()` - Now fetches FK info via `pragma_foreign_key_list` to preserve existing FKs during reconstruction

### 2. `applier/sqlite.py`

Changed methods to return reconstruction markers instead of raising errors:

```python
# Before
def _add_foreign_key_sql(self, table, details):
    raise NotImplementedError("...")

# After
def _add_foreign_key_sql(self, table, details):
    return f"__RECONSTRUCT_FK__{table}|{json.dumps(fk_info)}"
```

Methods updated:
- `_add_foreign_key_sql()` → returns `__RECONSTRUCT_FK__` marker
- `_drop_foreign_key_sql()` → returns `__RECONSTRUCT_DROP_FK__` marker
- `_drop_column_sql()` → returns `__RECONSTRUCT_DROP_COL__` marker

Updated:
- `_handle_table_reconstruction()` - Handles all marker types (FK add/drop, column drop)
- Marker detection changed from `__RECONSTRUCT__` to `__RECONSTRUCT` (prefix match)

### 3. `migrations.py`

The sync migration path (`apply_migrations_sync`) wasn't handling reconstruction markers - it just executed SQL directly.

Added sync versions of all reconstruction functions:
- `_handle_table_reconstruction_sync(conn, marker, operation)`
- `_get_full_table_schema_sync(conn, table)`
- `_reconstruct_table_sync(conn, table, new_columns)`
- `_generate_create_table_sql_sync(table, columns)`
- `_drop_column_sync(conn, table, column)`
- `_add_foreign_key_sync(conn, table, column, references, ...)`
- `_drop_foreign_key_sync(conn, table, column)`
- `_alter_column_sync(conn, table, column, changes)`

Also fixed:
- DateTime default values: `datetime('now')` → `(datetime('now'))` (SQLite requires parens around function calls)
- Added `DROP TABLE IF EXISTS` before creating temp table (cleanup from failed migrations)

## Current Status: RESOLVED

The `users_new` table reference issue described below was resolved by the reconstruction architecture rework in `abstractions/reconstruction.py`. The new implementation uses per-operation execution with fresh introspection, avoiding stale table references.

Additionally, a differ-format bug was fixed (2026-03-06): the differ produces `{"from": old, "to": new}` dicts for changes, which were being assigned directly as column values instead of extracting the `"to"` value. This caused crashes during `alter_column` reconstruction (e.g., `'dict' object has no attribute 'lower'`). A fail-fast `_validate_columns()` check now runs before any destructive operations.

### Original issue (historical):

Migration was failing with:
```
ERROR: Failed at operation 7: no such table: users_new
```

Root cause: Operations and SQL were generated upfront before execution. After reconstruction renamed `users_new` → `users`, cached operations still referenced the old temp table name. Fixed by per-operation fresh introspection model.

## Important Notes

1. **Changes ported to source repo** - The reconstruction logic now lives in `abstractions/reconstruction.py` with both async and sync execution paths.

2. **TursoApplier updated** - The Turso applier (`applier/turso.py`) now uses `_execute_with_reconstruction_sync()` with the same logic as the SQLite applier, including the differ-format fix.

3. **Tests written** - See `tests/unit/test_reconstruction.py`, `tests/unit/test_table_reconstruction.py`, and `tests/unit/test_sqlite_applier_reconstruction.py`.

## Files Modified

All in `/Users/adam/dev/multicardz/vendor/declaro_persistum/src/declaro_persistum/`:

- `abstractions/table_reconstruction.py` - Added FK and column drop functions
- `applier/sqlite.py` - Changed to use reconstruction markers
- `migrations.py` - Added sync reconstruction handling
