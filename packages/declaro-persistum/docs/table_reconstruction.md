# Table Reconstruction for SQLite/Turso

## Overview

SQLite and Turso Database don't support many `ALTER TABLE` operations natively. The only way to change column definitions or foreign key relationships is through **table reconstruction**.

This abstraction provides a safe, transactional way to perform table reconstruction while preserving:
- Data integrity
- Foreign key relationships
- Indexes and constraints
- Transactional safety

## Operations Requiring Reconstruction

The following operations cannot be performed directly in SQLite/Turso and require table reconstruction:

- **alter_column** - Change column type, nullability, or default value
- **add_foreign_key** - Add FK constraint to existing column
- **drop_foreign_key** - Remove FK constraint from column
- **drop_column** - Remove column (SQLite 3.35.0+ has native support, but reconstruction is used for consistency)

These operations are automatically detected by the applier and handled via reconstruction.

## Implementation

The table reconstruction process follows SQLite's recommended approach:

1. **Fresh introspection** - Get current table schema
2. **Disable foreign keys** temporarily during reconstruction
3. **Create new table** with desired schema (with `_new` suffix)
4. **Copy data** from old table to new table (common columns only)
5. **Drop old table**
6. **Rename new table** to original name
7. **Recreate indexes** (explicit CREATE INDEX statements only)
8. **Re-enable foreign keys** and verify constraints

## Usage

### Automatic Reconstruction (Recommended)

The SQLite and Turso appliers automatically detect operations requiring reconstruction and handle them transparently:

```python
from declaro_persistum.applier.sqlite import SQLiteApplier

operations = [
    {
        "op": "alter_column",
        "table": "users",
        "details": {
            "column": "email",
            "changes": {"nullable": False},
        },
    },
    {
        "op": "add_foreign_key",
        "table": "posts",
        "details": {
            "column": "user_id",
            "references": {"table": "users", "column": "id"},
            "on_delete": "cascade",
        },
    },
]

# Async execution (SQLite with aiosqlite)
applier = SQLiteApplier()
result = await applier.apply(conn, operations, [0, 1])

# Sync execution (Turso Database with pyturso)
from declaro_persistum.applier.turso import TursoApplier
applier = TursoApplier()
result = applier.apply_sync(conn, operations, [0, 1])
```

### Direct API (Advanced)

For advanced use cases, you can call the reconstruction functions directly:

```python
from declaro_persistum.abstractions.reconstruction import (
    execute_reconstruction_async,
    execute_reconstruction_sync,
    get_reconstruction_columns,
)

# Async reconstruction (SQLite/LibSQL)
new_columns = {
    "id": {"type": "INTEGER", "primary_key": True},
    "email": {"type": "TEXT", "nullable": False},
    "name": {"type": "TEXT", "nullable": True},
}
await execute_reconstruction_async(conn, "users", new_columns)

# Sync reconstruction (Turso Database)
execute_reconstruction_sync(conn, "users", new_columns)

# Compute new schema from operation
operation = {
    "op": "alter_column",
    "table": "users",
    "details": {
        "column": "email",
        "changes": {"nullable": False},
    },
}
new_columns = get_reconstruction_columns(current_columns, operation)
```

## Safety Features

### Transactional

All reconstruction operations are wrapped in transactions. If any step fails, the entire operation is rolled back, leaving the original table intact.

### Foreign Key Preservation

Foreign keys are:
1. Temporarily disabled during reconstruction
2. Re-enabled after completion
3. Verified via `PRAGMA foreign_key_check`

If any foreign key violations are detected after reconstruction, an exception is raised and the transaction is rolled back.

### Data Validation

The abstraction validates that data is compatible with new constraints:
- Cannot make a column NOT NULL if it contains NULL values
- Type conversions follow SQLite's type affinity rules

### Index Preservation

Explicitly created indexes (via `CREATE INDEX`) are preserved. Auto-generated indexes (from UNIQUE constraints, PRIMARY KEY) are not recreated separately since they're part of the table definition.

## Limitations

### Not Preserved

The following are **not preserved** during reconstruction:
- Triggers (must be recreated manually)
- Views that reference the table (may break)
- Auto-generated index names may change

### Performance

Table reconstruction requires copying all data, so it can be slow for large tables. For tables with millions of rows, consider:
- Running during maintenance windows
- Monitoring the operation duration
- Testing on a copy of production data first

Note: No batching optimization is performed. Each operation requiring reconstruction performs fresh introspection and independent execution.

### Concurrency

SQLite has limited concurrency support. Table reconstruction acquires a write lock on the table, blocking all other operations during reconstruction.

### Turso-Specific Considerations

**Turso Database (Rust/pyturso) differences:**
- Uses synchronous API (no async/await)
- CHECK constraints are now supported natively (as of early 2026) and included in reconstruction SQL
- Most PRAGMAs needed for reconstruction are supported (table_info, index_list, index_info, foreign_keys); `foreign_key_list` still requires emulation
- Same reconstruction logic applies, but executed via `execute_reconstruction_sync()`

**Turso Cloud (libSQL/libsql_experimental):**
- Uses async API like SQLite
- Full SQLite compatibility including CHECK constraints
- Executed via `execute_reconstruction_async()`

## Execution Model

### Per-Operation Execution

Table reconstruction uses **per-operation execution** rather than batching:

1. Each operation requiring reconstruction is executed independently
2. Fresh introspection is performed before each reconstruction
3. No schema caching or state is maintained between operations
4. Each operation commits independently within the parent transaction

This ensures:
- Accurate schema state for each operation
- No state corruption from failed operations
- Simplified error handling and recovery
- Predictable behavior when operations interact

### Async vs Sync Execution

The implementation provides both async and sync execution paths:

**Async (SQLite/LibSQL):**
```python
from declaro_persistum.abstractions.reconstruction import execute_reconstruction_async

# For aiosqlite, libsql_experimental connections
await execute_reconstruction_async(conn, table_name, new_columns)
```

**Sync (Turso Database):**
```python
from declaro_persistum.abstractions.reconstruction import execute_reconstruction_sync

# For pyturso connections
execute_reconstruction_sync(conn, table_name, new_columns)
```

The SQLite applier uses async execution, while the Turso applier uses sync execution to match their respective connection APIs.

## Architecture

### File Structure

```
declaro_persistum/
├── abstractions/
│   ├── table_reconstruction.py    # Legacy async-only implementation
│   └── reconstruction.py           # New shared implementation (async + sync)
├── applier/
│   ├── shared.py                   # Shared pure SQL generation (sqlite + turso)
│   ├── sqlite.py                   # Integration with SQLite applier (async)
│   └── turso.py                    # Integration with Turso applier (sync)
└── tests/
    ├── unit/
    │   ├── test_table_reconstruction.py          # Core tests (legacy)
    │   └── test_sqlite_applier_reconstruction.py # Integration tests
    └── bdd/
        ├── features/
        │   └── table_reconstruction.feature       # BDD scenarios
        └── steps/
            └── test_table_reconstruction_steps.py # BDD step definitions
```

### Design Patterns

Following `pragma_compat.py` patterns:
- Pure functions for SQL generation
- Separate async/sync execution functions
- No classes with state
- Comprehensive error handling
- Detailed logging for debugging

### Integration Points

The abstraction is used by:
1. **SQLite Applier** (`applier/sqlite.py`) - Async execution via `execute_reconstruction_async()`
2. **Turso Applier** (`applier/turso.py`) - Sync execution via `execute_reconstruction_sync()`
3. **Direct API** - Can be called directly for advanced use cases

## References

- [SQLite ALTER TABLE Documentation](https://www.sqlite.org/lang_altertable.html)
- [Turso Database Compatibility](https://github.com/tursodatabase/turso/blob/main/COMPAT.md)
- SQLite's recommended approach: https://www.sqlite.org/lang_altertable.html#making_other_kinds_of_table_schema_changes
