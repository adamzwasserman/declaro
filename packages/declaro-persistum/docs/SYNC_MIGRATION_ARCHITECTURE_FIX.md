# Sync Migration Architecture Fix

> **Note (2026-03-08)**: The synchronous pool and connection API was removed from declaro-persistum. This document describes a fix that was subsequently superseded by the full removal of all sync surface area. `SyncConnectionPool`, `SyncLibSQLPool`, `SyncSQLitePool`, `SyncTursoPool`, and all sync connection types were deleted. declaro-persistum is async-only going forward.

## Summary

Fixed the sync migration architecture in declaro-persistum to properly delegate execution to the applier layer rather than duplicating logic in migrations.py.

## Changes Made

### 1. SQLiteApplier - Added Real `apply_sync()` Implementation

**File**: `src/declaro_persistum/applier/sqlite.py`

- Replaced `NotImplementedError` stub with full synchronous implementation
- Uses stdlib `sqlite3.Connection` type
- Per-operation execution with reconstruction support via `_execute_with_reconstruction_sync()`
- Handles enum value population inline (moved from migrations.py)
- Transactional DDL with proper rollback on errors

**New method**: `_execute_with_reconstruction_sync()`
- Synchronous version of `_execute_with_reconstruction()`
- Uses direct `PRAGMA table_info()` calls instead of async pragma_compat
- Delegates to `execute_reconstruction_sync()` from abstractions layer
- Supports: alter_column, add_foreign_key, drop_foreign_key

### 2. TursoApplier - Added Enum Value Population

**File**: `src/declaro_persistum/applier/turso.py`

- Added `target_schema` parameter to `apply_sync()` method
- Implemented enum value population for `_dp_enum_*` tables after creation
- Same logic as SQLiteApplier for consistency

### 3. PostgreSQLApplier - Added Protocol Compliance

**File**: `src/declaro_persistum/applier/postgresql.py`

- Added `apply_sync()` method that raises `NotImplementedError`
- PostgreSQL is async-only in this codebase
- Maintains protocol compliance

### 4. MigrationApplier Protocol - Updated Signature

**File**: `src/declaro_persistum/applier/protocol.py`

- Added `target_schema: Any = None` parameter to `apply_sync()` protocol method
- Updated docstring to document the parameter

### 5. Migrations - Simplified `_apply_sync()`

**File**: `src/declaro_persistum/migrations.py`

- Removed duplicated execution logic (70+ lines → 10 lines)
- Now simply delegates to `applier.apply_sync()`
- Passes `target_schema` through for enum population
- Changed return type from `dict[str, Any]` to `ApplyResult` for type safety
- Added imports for `ApplyResult` and `Operation` types

## Architecture Benefits

### Before
```
migrations.py::_apply_sync()
  ├─ Manually executes SQL statements
  ├─ Manually handles enum value population
  ├─ Manually manages transactions
  └─ Duplicates logic from applier layer
```

### After
```
migrations.py::_apply_sync()
  └─ Delegates to applier.apply_sync()
      ├─ SQLiteApplier handles execution + reconstruction + enums
      ├─ TursoApplier handles execution + reconstruction + enums
      └─ PostgreSQLApplier raises NotImplementedError (async-only)
```

## Key Principles

1. **Single Responsibility**: Appliers execute operations, migrations.py orchestrates
2. **No Duplication**: Logic exists in one place (applier layer)
3. **Consistency**: Async and sync paths use same architecture
4. **Type Safety**: Proper protocol compliance and type annotations

## Testing

All changes verified with:
- Type checking (mypy) - no errors
- Unit tests (test_sqlite_applier_reconstruction.py) - all passing
- Manual end-to-end tests:
  - Basic table creation
  - Table reconstruction (add_foreign_key)
  - Enum value population

## Files Modified

1. `src/declaro_persistum/applier/sqlite.py` - Added real apply_sync()
2. `src/declaro_persistum/applier/turso.py` - Added enum population
3. `src/declaro_persistum/applier/postgresql.py` - Added protocol stub
4. `src/declaro_persistum/applier/protocol.py` - Updated signature
5. `src/declaro_persistum/migrations.py` - Simplified delegation

## Migration Guide

No breaking changes. Existing code will continue to work as before, but now:
- `apply_migrations_sync()` properly uses applier layer
- Enum values populate correctly in sync migrations
- SQLite reconstruction works in sync mode
