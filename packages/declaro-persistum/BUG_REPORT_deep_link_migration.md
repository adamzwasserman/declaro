---
type: bug
priority: 1
reporter: downstream consumer
date: 2026-03-15
---

# Migration batch aborts on unsupported operation, preventing valid ADD COLUMN

## Problem

`apply_migrations_async()` correctly detects a new column but fails because it batches all 47 detected schema differences into a single transaction. One unsupported operation (e.g., `add_foreign_key` on pyturso/SQLite) causes `MigrationError`, which aborts the entire batch — including the `ADD COLUMN` that would have succeeded independently.

## Observed Behavior

```
INFO:declaro_persistum.migrations:Loading schema from .../project_tables.py
INFO:declaro_persistum.migrations:Found 47 schema differences
INFO:declaro_persistum.migrations:  - add_index on table import_batches
INFO:declaro_persistum.migrations:  - add_foreign_key on table comments
  ... (43 more index/FK/alter_column operations) ...
INFO:declaro_persistum.migrations:  - add_column on table cards        <-- needed
INFO:declaro_persistum.migrations:  - add_index on table cards
  ...
ERROR: Failed to execute operation
declaro_persistum.exceptions.MigrationError: Failed to execute operation
```

The pool factory catches the error and continues with the old schema. The `add_column` is never applied.

On subsequent pool creations, the same 47 differences are detected again, the same operation fails again, and the column is never added. The migration is stuck.

## Root Cause

1. The schema defines foreign keys and indexes that pyturso/SQLite cannot apply (e.g., `ADD FOREIGN KEY` is not supported in SQLite `ALTER TABLE`)
2. These unsupported operations accumulate as permanent "phantom" differences
3. Every migration attempt re-detects them and fails on them
4. Valid operations (like `ADD COLUMN`) are bundled in the same batch and never execute

## Expected Behavior

Any of these would fix it:
1. **Best**: Apply each operation independently — skip failures, continue with the rest, report which succeeded/failed
2. **Good**: Skip operations known to be unsupported on the current dialect before attempting them
3. **Acceptable**: Order operations so safe ones (`ADD COLUMN`, `CREATE TABLE`) run first, then attempt riskier ones (`ADD FOREIGN KEY`, `ALTER COLUMN`)

## Steps to Reproduce

1. Create a schema with tables that define foreign keys and indexes
2. Create a database using pyturso backend — some FK/index operations will silently fail or be ignored
3. Add a new nullable field to one of the models
4. Call `apply_migrations_async(pool, "sqlite", schema_path, expand_enums=True)`
5. Observe: 47 differences detected, batch fails on an FK operation, `ADD COLUMN` never applied

## Environment

- Database backend: pyturso (SQLite-compatible, no `ALTER TABLE ADD FOREIGN KEY` support)
- Migration call: `apply_migrations_async(pool, "sqlite", schema_path, expand_enums=True)`
