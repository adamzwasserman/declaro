# Table Reconstruction Architecture

**Document Version**: 1.1
**Date**: 2026-02-01
**Updated**: 2026-03-06
**Status**: Implemented

---

**IMPLEMENTATION STATUS**: COMPLETE
**LAST VERIFIED**: 2026-03-06
**IMPLEMENTATION EVIDENCE**:
- Core: `src/declaro_persistum/abstractions/reconstruction.py`
- Legacy: `src/declaro_persistum/abstractions/table_reconstruction.py`
- SQLite Integration: `src/declaro_persistum/applier/sqlite.py` (lines 132-262)
- Turso Integration: `src/declaro_persistum/applier/turso.py` (lines 134-239)
- Tests: `tests/unit/test_table_reconstruction.py`, `tests/unit/test_sqlite_applier_reconstruction.py`
- BDD: `tests/bdd/features/table_reconstruction.feature`

---

## Executive Summary

Table reconstruction is a fundamental technique for working with SQLite and Turso Database, which have limited `ALTER TABLE` support. This architecture defines a pure functional abstraction that:

1. Provides transparent table reconstruction for operations unsupported by SQLite/Turso
2. Supports both async (SQLite, LibSQL) and sync (Turso Database) execution
3. Performs fresh introspection before each reconstruction (no batching)
4. Maintains data integrity and foreign key constraints during reconstruction
5. Integrates seamlessly with the migration applier protocol

**Scope**: SQLite and Turso Database backends. PostgreSQL does not use reconstruction (native ALTER TABLE support).

---

## Problem Statement

### Database Limitations

SQLite and Turso Database have limited `ALTER TABLE` support:

**Operations NOT supported:**
- `ALTER COLUMN` (change type, nullability, default)
- `ADD CONSTRAINT FOREIGN KEY` (after table creation)
- `DROP CONSTRAINT FOREIGN KEY`
- `DROP CONSTRAINT` (any constraint type)

**Operations with limited support:**
- `DROP COLUMN` (SQLite 3.35.0+ only)
- `RENAME COLUMN` (SQLite 3.25.0+ only)

### SQLite's Recommended Solution

SQLite documentation recommends **table reconstruction**:

1. Create new table with desired schema
2. Copy data from old to new
3. Drop old table
4. Rename new table to original name
5. Recreate indexes

Reference: https://www.sqlite.org/lang_altertable.html#making_other_kinds_of_table_schema_changes

### Design Goals

1. **Transparent** - Appliers automatically detect and use reconstruction
2. **Safe** - Transactional with foreign key integrity checks
3. **Dual execution** - Support both async (aiosqlite, libsql_experimental) and sync (pyturso)
4. **Stateless** - Pure functions with fresh introspection per operation
5. **Predictable** - No caching or batching optimization

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Migration Applier                         │
│                 (SQLiteApplier / TursoApplier)               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Detects operation requiring reconstruction
                         │ (_requires_reconstruction() → True)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                 Applier Integration Layer                    │
│                                                              │
│  SQLite: _execute_with_reconstruction() [async]             │
│  Turso:  _execute_with_reconstruction_sync() [sync]         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ 1. Fresh introspection (PRAGMA table_info)
                         │ 2. Build new column definitions
                         │ 3. Call execution function
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Reconstruction Core (abstractions/)             │
│                                                              │
│  Pure Functions:                                            │
│    - generate_create_table_sql()                            │
│    - generate_data_copy_sql()                               │
│    - get_reconstruction_columns()                           │
│                                                              │
│  Execution Functions:                                       │
│    - execute_reconstruction_async()  [SQLite/LibSQL]        │
│    - execute_reconstruction_sync()   [Turso Database]       │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Operation → Applier Detection → Fresh Introspection → Schema Computation → SQL Generation → Execution
    │            │                      │                    │                  │             │
    │            │                      │                    │                  │             │
    │            ▼                      ▼                    ▼                  ▼             ▼
    │      _requires_        PRAGMA table_info    get_reconstruction_   generate_create_  PRAGMA fk=OFF
    │      reconstruction()                       columns()             table_sql()       CREATE TABLE
    │            │                                      │                                  COPY DATA
    │            │                                      │                                  DROP OLD
    │            │                                      │                                  RENAME
    │            │                                      │                                  RECREATE INDEX
    │            │                                      │                                  PRAGMA fk=ON
    │            │                                      │                                  FK CHECK
    │            │                                      │
    │            └──────────────────────────────────────┘
    │
    └─ Result (success/failure + executed SQL)
```

### Per-Operation Execution Model

**Critical Design Decision**: Each operation requiring reconstruction executes independently.

```python
# Per-operation execution in SQLiteApplier.apply()
for op_idx in execution_order:
    operation = operations[op_idx]

    if self._requires_reconstruction(operation):
        # Fresh introspection BEFORE each operation
        rows = await pragma_table_info(connection, table)
        current_columns = parse_columns(rows)

        # Compute new schema for THIS operation only
        new_columns = apply_operation_to_schema(current_columns, operation)

        # Execute reconstruction
        await execute_reconstruction_async(connection, table, new_columns)
    else:
        # Direct SQL execution
        sql = self.generate_operation_sql(operation)
        await connection.execute(sql)
```

**Why no batching?**
- Simplifies error handling (each operation is atomic)
- Ensures accurate schema state (no stale introspection)
- Prevents state corruption from partial failures
- Matches the execution model of PostgreSQL applier (per-operation)

**Trade-off**: Performance vs correctness. We choose correctness.

---

## Core Abstractions

### 1. SQL Generation (Pure Functions)

**Module**: `abstractions/reconstruction.py`

```python
def generate_create_table_sql(
    table_name: str,
    columns: dict[str, Column],
) -> str:
    """
    Generate CREATE TABLE statement from column definitions.

    Pure function - no side effects, no database access.
    Foreign keys are embedded in column definitions.
    """
```

**Design Notes:**
- Pure function (testable without database)
- Foreign keys specified via `references` field in Column
- Supports composite primary keys
- CHECK constraints included in output (all backends support CHECK natively)

```python
def generate_data_copy_sql(
    table_from: str,
    table_to: str,
    common_columns: list[str],
) -> str:
    """
    Generate INSERT...SELECT for data migration.

    Only copies columns that exist in both tables.
    """
```

```python
def get_reconstruction_columns(
    current_columns: dict[str, Column],
    operation: Operation,
) -> dict[str, Column]:
    """
    Compute new column definitions from operation.

    Handles:
    - alter_column: Apply changes to column def
    - drop_column: Remove from dict
    - add_foreign_key: Add references field
    - drop_foreign_key: Remove references field

    NOTE: The differ produces {"from": old, "to": new} dicts for changes.
    This function extracts the "to" value before applying changes.
    """
```

### 2. Execution Functions

**Async Execution** (SQLite, LibSQL):

```python
async def execute_reconstruction_async(
    connection: Any,
    table_name: str,
    new_columns: dict[str, Column],
    *,
    preserve_data: bool = True,
) -> None:
    """
    Execute table reconstruction asynchronously.

    Steps:
    1. Fresh introspection (PRAGMA table_info)
    2. Disable foreign keys (PRAGMA foreign_keys = OFF)
    3. Create temp table with new schema
    4. Copy data (common columns only)
    5. Drop old table
    6. Rename temp to original name
    7. Recreate indexes
    8. Re-enable foreign keys
    9. Verify FK integrity (PRAGMA foreign_key_check)

    Raises on failure - caller handles transaction rollback.
    """
```

**Sync Execution** (Turso Database):

```python
def execute_reconstruction_sync(
    connection: Any,
    table_name: str,
    new_columns: dict[str, Column],
    *,
    preserve_data: bool = True,
) -> None:
    """
    Same as execute_reconstruction_async but synchronous.

    For pyturso connections (Turso Database Rust implementation).
    """
```

**Key Difference**: Async uses `await`, sync does not. Otherwise identical logic.

---

## Integration with Appliers

### SQLite Applier

**File**: `applier/sqlite.py`

**Detection**:
```python
def _requires_reconstruction(self, operation: Operation) -> bool:
    op_type = operation["op"]
    return op_type in ("add_foreign_key", "drop_foreign_key", "alter_column")
```

**Execution**:
```python
async def _execute_with_reconstruction(
    self, connection: Any, operation: Operation
) -> None:
    # 1. Fresh introspection
    rows = await pragma_table_info(connection, table)
    columns = parse_to_column_dict(rows)

    # 2. Apply operation changes to schema
    if op_type == "alter_column":
        column = details["column"]
        changes = details["changes"]
        for key, value in changes.items():
            columns[column][key] = value

    elif op_type == "add_foreign_key":
        column = details["column"]
        ref = details["references"]
        columns[column]["references"] = f"{ref['table']}.{ref['column']}"
        # ... on_delete, on_update

    # 3. Reconstruct
    await reconstruct_table(connection, table, columns)  # Legacy API
    # OR
    await execute_reconstruction_async(connection, table, columns)  # New API
```

### Turso Applier

**File**: `applier/turso.py`

**Detection**: Same as SQLite (inherited logic)

**Execution**:
```python
def _execute_with_reconstruction_sync(
    self, connection: Any, operation: Operation
) -> None:
    # Same logic as SQLite but synchronous
    cursor = connection.execute(f"PRAGMA table_info('{table}')")
    rows = cursor.fetchall()
    columns = parse_to_column_dict(rows)

    # ... apply operation changes ...

    # Synchronous execution
    execute_reconstruction_sync(connection, table, columns)
```

---

## Foreign Key Handling

### Why Foreign Keys Are Critical

Foreign keys create dependencies between tables. Reconstruction must:
1. Preserve FK constraints in the new table
2. Temporarily disable FK checks during reconstruction
3. Verify FK integrity after reconstruction

### Implementation

**Disable during reconstruction:**
```python
# Before reconstruction
fk_cursor = await connection.execute("PRAGMA foreign_keys")
fk_enabled = fk_cursor.fetchone()[0]

await connection.execute("PRAGMA foreign_keys = OFF")
```

**Embed in column definition:**
```python
# Column definition with FK
column = {
    "type": "INTEGER",
    "nullable": False,
    "references": "users.id",
    "on_delete": "cascade",
}

# Generated SQL
CREATE TABLE posts (
    user_id INTEGER NOT NULL REFERENCES "users"("id") ON DELETE CASCADE
)
```

**Verify after reconstruction:**
```python
# Re-enable and check
await connection.execute("PRAGMA foreign_keys = ON")
check_cursor = await connection.execute(f'PRAGMA foreign_key_check("{table}")')
violations = await check_cursor.fetchall()

if violations:
    raise ValueError(f"FK violations: {violations}")
```

---

## Index Preservation

### Strategy

Only **explicitly created indexes** are preserved (via `CREATE INDEX`).

**Auto-generated indexes** are NOT preserved separately:
- PRIMARY KEY indexes
- UNIQUE constraint indexes

These are recreated automatically as part of the table definition.

### Implementation

```python
async def _get_table_indexes_async(connection: Any, table_name: str) -> list[str]:
    index_list = await pragma_index_list(connection, table_name)
    index_sqls = []

    for row in index_list:
        index_name = row[1]
        origin = row[3]

        # Skip auto-generated (origin = 'u' for unique, 'pk' for primary key)
        if origin in ("u", "pk"):
            continue

        # Get CREATE INDEX statement from sqlite_master
        cursor = await connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            (index_name,)
        )
        result = await cursor.fetchone()
        if result and result[0]:
            index_sqls.append(result[0])

    return index_sqls
```

**Recreate after rename:**
```python
for index_sql in indexes:
    # Replace temp table name with original
    recreate_sql = index_sql.replace(f'"{temp_table}"', f'"{table_name}"')
    await connection.execute(recreate_sql)
```

---

## Transaction Handling

### Applier Transaction Mode

Both SQLite and Turso appliers declare:
```python
def get_transaction_mode(self) -> Literal["all_or_nothing", "per_operation"]:
    return "all_or_nothing"
```

This means DDL is transactional.

### Reconstruction Within Transaction

```python
# In applier.apply()
try:
    await connection.execute("BEGIN")  # Or implicit BEGIN in aiosqlite

    for operation in operations:
        if requires_reconstruction:
            await execute_reconstruction_async(connection, ...)
        else:
            await connection.execute(sql)

    await connection.commit()
except Exception:
    await connection.rollback()
    raise
```

**Key Point**: Reconstruction does NOT start its own transaction. It executes within the applier's transaction.

If reconstruction fails:
- Exception is raised
- Applier catches and calls `rollback()`
- Database reverts to state before migration started
- Original table is intact

---

## Error Handling

### Error Propagation

```python
async def execute_reconstruction_async(...):
    try:
        # ... reconstruction steps ...
    except Exception as e:
        logger.error(f"Reconstruction failed: {e}")
        # Re-enable foreign keys before re-raising
        try:
            if fk_enabled:
                await connection.execute("PRAGMA foreign_keys = ON")
        except Exception:
            pass
        raise  # Propagate to applier for transaction rollback
```

### Common Error Scenarios

1. **Foreign key violations**:
   - Detected by `PRAGMA foreign_key_check`
   - Raised as `ValueError` with violation details
   - Transaction rolls back

2. **Data copy failures**:
   - Type incompatibility (e.g., TEXT → INTEGER with non-numeric data)
   - Null values in column becoming NOT NULL
   - Raised as SQLite error
   - Transaction rolls back

3. **Index recreation failures**:
   - Missing columns in index definition
   - Duplicate index names
   - Raised as SQLite error
   - Transaction rolls back

### Recovery

On failure, the original table remains intact because:
1. Reconstruction creates `table_new`
2. Original table is only dropped AFTER new table is ready
3. If any step fails, transaction rolls back
4. `table_new` is discarded
5. Original table is untouched

---

## Turso-Specific Considerations

### Turso Cloud (libSQL) vs Turso Database (Rust)

**Two different backends:**

| Backend | Package | API | SQL Parser | Reconstruction |
|---------|---------|-----|------------|----------------|
| Turso Cloud | libsql_experimental | Async | SQLite-compatible | `execute_reconstruction_async()` |
| Turso Database | pyturso | Sync | Rust (limited) | `execute_reconstruction_sync()` |

### Turso Database CHECK Support

**CHECK constraints**: Turso Database (Rust) now supports CHECK constraints natively (confirmed in COMPAT.md as of early 2026).

The Turso applier emits CHECK clauses directly in SQL, identical to the SQLite applier. No stripping or Python-side emulation is needed.

```python
# In turso.py _column_definition()
# CHECK constraint - Turso now supports CHECK natively
if "check" in col:
    parts.append(f"CHECK ({col['check']})")
```

**Result**: Reconstruction proceeds with CHECK constraints preserved in SQL across all backends.

### PRAGMA Support

Turso Database may have limited PRAGMA support compared to SQLite. The reconstruction abstraction uses only essential PRAGMAs:

- `PRAGMA foreign_keys` - Supported
- `PRAGMA table_info` - Supported
- `PRAGMA index_list` - Supported
- `PRAGMA foreign_key_check` - Supported

These are the minimum required for safe reconstruction.

---

## Testing Strategy

### Unit Tests

**File**: `tests/unit/test_table_reconstruction.py`

- Pure function tests (SQL generation)
- Schema computation tests
- Column mapping tests

**File**: `tests/unit/test_sqlite_applier_reconstruction.py`

- Integration tests with SQLite applier
- Foreign key add/drop scenarios
- Column alteration scenarios
- Error handling tests

### BDD Tests

**File**: `tests/bdd/features/table_reconstruction.feature`

```gherkin
Feature: Table Reconstruction for SQLite Limitations

  Scenario: Alter column nullability requires reconstruction
    Given a table "users" with column "email" type TEXT nullable
    When I alter column "email" to NOT NULL
    Then the table should be reconstructed
    And the column "email" should be NOT NULL
    And all data should be preserved
    And foreign key constraints should be intact
```

### Test Coverage

- Column alterations (type, nullability, default)
- Foreign key add/drop
- Index preservation
- Data preservation
- Foreign key integrity
- Error scenarios (FK violations, type mismatches)
- Transaction rollback

---

## Performance Characteristics

### Time Complexity

**Per operation:**
- Introspection: O(columns)
- Create temp table: O(columns)
- Data copy: O(rows * columns)
- Drop old: O(1)
- Rename: O(1)
- Index recreation: O(indexes * rows * log(rows))

**Total**: O(rows * columns) dominated by data copy.

### Space Complexity

**Peak disk usage**: 2x table size (original + temp table exist simultaneously).

**After completion**: 1x table size (temp table dropped after rename).

### Optimization Trade-offs

**No batching**: Each operation performs independent reconstruction.

**Cost**: N operations = N reconstructions = N * O(rows * columns)

**Benefit**:
- Correct schema state for each operation
- Simplified error handling
- No state management between operations

**When batching would help**:
- Multiple FK additions to same table
- Multiple column alterations to same table
- Could be done in single reconstruction

**Why we don't batch**:
- Complexity of tracking operation groups
- Risk of state corruption
- Marginal benefit for typical migrations (few operations per table)

---

## Design Decisions

### 1. Pure Functions for SQL Generation

**Decision**: Separate SQL generation from execution.

**Rationale**:
- Testable without database
- Reusable between async/sync
- Clear separation of concerns

**Alternative Considered**: Inline SQL generation in execution functions.
**Rejected Because**: Harder to test, duplicate logic between async/sync.

### 2. Fresh Introspection Per Operation

**Decision**: No caching of schema state between operations.

**Rationale**:
- Previous operation may have changed schema
- Caching requires invalidation logic
- Fresh state is always correct

**Alternative Considered**: Cache schema and invalidate on changes.
**Rejected Because**: Added complexity, risk of stale state, marginal performance benefit.

### 3. Separate Async/Sync Functions

**Decision**: Duplicate logic with `execute_reconstruction_async()` and `execute_reconstruction_sync()`.

**Rationale**:
- Clean APIs for each connection type
- No runtime async detection
- Type-safe (async/sync not mixed)

**Alternative Considered**: Single function with runtime async detection.
**Rejected Because**: Type safety issues, harder to test, violates Python async conventions.

### 4. Embedding Foreign Keys in Column Definitions

**Decision**: Use `references` field in Column TypedDict.

**Rationale**:
- Matches SQLite inline FK syntax
- Simplifies SQL generation
- Natural data model for FK columns

**Alternative Considered**: Separate FK constraint dict.
**Rejected Because**: Requires table-level constraint tracking, more complex SQL generation.

### 5. No Trigger Preservation

**Decision**: Triggers are NOT preserved during reconstruction.

**Rationale**:
- Triggers reference table by name
- Temp table has different name
- Triggers may interfere with data copy
- Migration layer should handle trigger recreation explicitly

**Alternative Considered**: Parse triggers, rename references, recreate.
**Rejected Because**: Complex trigger SQL parsing, edge cases, not required for core use case.

---

## Differ Format Handling (Fixed 2026-03-06)

### Problem

The differ (`differ/core.py`) produces change dicts in `{"from": old_value, "to": new_value}` format for `alter_column` operations. The reconstruction code was blindly assigning these dicts as column property values instead of extracting the `"to"` value. This caused `_map_type()` to crash with `'dict' object has no attribute 'lower'` when processing Literal type changes.

### Fix

All reconstruction paths now extract the `"to"` value from differ-format dicts:

```python
for key, value in changes.items():
    if isinstance(value, dict) and "to" in value:
        value = value["to"]
    columns[column][key] = value
```

This fix was applied in:
- `abstractions/reconstruction.py` — `get_reconstruction_columns()`
- `applier/sqlite.py` — both `_execute_with_reconstruction()` and `_execute_with_reconstruction_sync()`
- `applier/turso.py` — `_execute_with_reconstruction_sync()`

### Fail-Fast Validation

A `_validate_columns()` function was added to `reconstruction.py` that runs **before** any destructive operations (DROP TABLE). It catches invalid column definitions (e.g., dict types that weren't unwrapped) before data loss can occur. Both `execute_reconstruction_async()` and `execute_reconstruction_sync()` call this validation.

---

## Future Enhancements

### Potential Optimizations

1. **Batching detection**: Identify consecutive operations on same table, batch into single reconstruction.
   - **Complexity**: Medium
   - **Benefit**: Performance improvement for multiple operations per table
   - **Risk**: State management errors

2. **Partial data copy**: For large tables, copy in batches with progress reporting.
   - **Complexity**: Low
   - **Benefit**: Better UX for large tables
   - **Risk**: Transaction size limits

3. **Dry-run mode**: Generate SQL without executing.
   - **Complexity**: Low
   - **Benefit**: Better migration preview
   - **Risk**: None (already partially implemented in applier)

### Feature Additions

1. **Trigger preservation**: Parse and recreate triggers.
   - **Complexity**: High
   - **Benefit**: Complete schema preservation
   - **Risk**: Trigger SQL parsing edge cases

2. **View preservation**: Update views that reference reconstructed table.
   - **Complexity**: Medium
   - **Benefit**: Prevent view breakage
   - **Risk**: View SQL parsing complexity

---

## References

- [SQLite ALTER TABLE Documentation](https://www.sqlite.org/lang_altertable.html)
- [SQLite Table Reconstruction Guide](https://www.sqlite.org/lang_altertable.html#making_other_kinds_of_table_schema_changes)
- [Turso Database Repository](https://github.com/tursodatabase/turso)
- [Declaro Persistum README](../../README.md)
- Implementation: `src/declaro_persistum/abstractions/reconstruction.py`

---

## Appendix A: Operation Type Mapping

| Operation | Requires Reconstruction | SQLite Native Support | Turso Support |
|-----------|------------------------|----------------------|---------------|
| create_table | No | Yes | Yes |
| drop_table | No | Yes | Yes |
| rename_table | No | Yes | Yes |
| add_column | No | Yes | Yes |
| drop_column | **Yes** (for consistency) | SQLite 3.35.0+ | Yes |
| rename_column | No | SQLite 3.25.0+ | Yes |
| alter_column | **Yes** | No | No |
| add_foreign_key | **Yes** | No | No |
| drop_foreign_key | **Yes** | No | No |
| add_index | No | Yes | Yes |
| drop_index | No | Yes | Yes |

---

## Appendix B: Reconstruction SQL Example

**Given**: Table `users` with column `email TEXT NULL`
**Operation**: Make `email` NOT NULL

**Generated SQL**:

```sql
-- 1. Disable foreign keys
PRAGMA foreign_keys = OFF;

-- 2. Create temp table
CREATE TABLE "users_new" (
    "id" INTEGER PRIMARY KEY,
    "email" TEXT NOT NULL,
    "name" TEXT
);

-- 3. Copy data
INSERT INTO "users_new" (id, email, name)
SELECT id, email, name FROM "users";

-- 4. Drop old table
DROP TABLE "users";

-- 5. Rename temp table
ALTER TABLE "users_new" RENAME TO "users";

-- 6. Recreate indexes (if any)
CREATE INDEX "idx_users_email" ON "users" ("email");

-- 7. Re-enable foreign keys
PRAGMA foreign_keys = ON;

-- 8. Verify FK integrity
PRAGMA foreign_key_check("users");
```

---

## Appendix C: Column TypedDict Definition

```python
class Column(TypedDict, total=False):
    """Column definition for table reconstruction."""
    type: str                    # SQL type (e.g., "INTEGER", "TEXT")
    nullable: bool               # True = NULL allowed, False = NOT NULL
    primary_key: bool            # True = PRIMARY KEY
    unique: bool                 # True = UNIQUE constraint
    default: str                 # Default value SQL expression
    check: str                   # CHECK constraint expression
    references: str              # FK reference "table.column"
    on_delete: str               # FK on delete action ("cascade", etc.)
    on_update: str               # FK on update action
```

**Minimal Column**:
```python
{"type": "TEXT"}
```

**Full Column with FK**:
```python
{
    "type": "INTEGER",
    "nullable": False,
    "references": "users.id",
    "on_delete": "cascade",
    "on_update": "restrict",
}
```
