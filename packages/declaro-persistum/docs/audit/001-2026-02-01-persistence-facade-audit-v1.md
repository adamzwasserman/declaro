# Data Persistence Facade Audit Report

**Document Version**: 1.0
**Date**: 2026-02-01
**Audit Scope**: Entire declaro monorepo
**Auditor**: Architecture Governance

---

## Executive Summary

This audit verifies that ALL database persistence in the declaro monorepo goes through the declaro-persistum facade. declaro-persistum is designed as a FACADE pattern - all database access (PostgreSQL, SQLite, Turso Rust, LibSQL) must use its ConnectionPool and query interfaces.

**Overall Finding**: The codebase is PARTIALLY COMPLIANT with 4 significant violations found in the example applications within declaro-persistum itself.

---

## Packages Audited

| Package | Status | Notes |
|---------|--------|-------|
| declaro-persistum/src | COMPLIANT | Internal implementation (allowed to use drivers) |
| declaro-persistum/tests | COMPLIANT | Testing the facade itself (allowed) |
| declaro-persistum/examples | **VIOLATION** | 4 example apps bypass facade |
| declaro-persistum/docs | **DOCUMENTATION** | Shows direct driver usage in examples |
| declaro-tablix/src | COMPLIANT | Uses declaro_persistum.compat or protocol injection |
| declaro-tablix/examples | COMPLIANT | Uses ConnectionPool properly |
| declaro-advise/src | COMPLIANT | Empty package (only __init__.py) |
| declaro-observe/src | COMPLIANT | No database code |
| declaro-ximinez/src | COMPLIANT | No database code |

---

## Violations Found

### Critical Violations (Example Applications)

All four example applications in declaro-persistum have identical violations in their `db.py` files:

#### 1. examples/todo_app_native/db.py

**Violation Type**: Direct database driver imports and connection creation

**Lines with violations**:
- Line 111: `import aiosqlite`
- Line 112: `async with aiosqlite.connect(config.connection_string) as conn:`
- Line 116: `import asyncpg`
- Line 117: `conn = await asyncpg.connect(config.connection_string)`
- Line 124: `import libsql_experimental as libsql`
- Line 125-128: `conn = libsql.connect(...)`
- Line 135: `import turso`
- Line 136: `conn = turso.connect(config.connection_string)`
- Line 153: `import asyncpg` (ensure_postgres_database function)
- Line 157: `conn = await asyncpg.connect(...)` (ensure_postgres_database)
- Line 190: `import asyncpg` (test_connection function)
- Line 293-302: Driver availability checks

**Impact**: Example applications demonstrate ANTI-PATTERNS instead of proper facade usage.

#### 2. examples/todo_app_django_style/db.py
Identical violations as todo_app_native/db.py

#### 3. examples/todo_app_prisma_style/db.py
Identical violations as todo_app_native/db.py

#### 4. examples/todo_app_sqlalchemy/db.py
Identical violations as todo_app_native/db.py

### Documentation Issues

#### docs/usage.md

The usage documentation shows direct driver usage in code examples:
- Line 637: `import asyncpg`
- Line 641: `conn = await asyncpg.connect(...)`
- Line 750: `import aiosqlite`
- Line 754: `async with aiosqlite.connect(...)`
- Line 761: `import libsql_experimental as libsql`
- Line 764: `conn = libsql.connect(...)`
- Line 776: `conn = await asyncpg.connect(...)`

**Context**: These appear in the "Programmatic Usage" section for introspection. While this is acceptable for demonstrating Inspector usage (which operates at a lower level than the query layer), the documentation should clarify that this is an advanced/internal pattern and NOT for application data access.

---

## Compliant Patterns Found

### declaro-tablix (Exemplary Usage)

**File**: `/Users/adam/dev/declaro/packages/declaro-tablix/examples/demo/app.py`

```python
from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query import execute, raw

# Proper initialization
pool = await ConnectionPool.sqlite(str(DB_PATH))

# Proper query execution
async with pool.acquire() as conn:
    await execute(raw("SELECT ..."), conn)
```

This is the CORRECT pattern for application code.

### declaro-tablix/customization/persistence.py

Uses `declaro_persistum.compat.SessionLocal` which is a compatibility shim that wraps ConnectionPool. This is COMPLIANT as it goes through the facade.

---

## Recommended Fixes

### Priority 1: Fix Example Applications

The example applications are the most critical to fix because:
1. They serve as reference implementations for users
2. They demonstrate anti-patterns that users will copy
3. They undermine the facade architecture we're documenting

**Fix**: Refactor all `examples/todo_app_*/db.py` files to use `ConnectionPool`:

```python
# BEFORE (violation)
import aiosqlite
async with aiosqlite.connect(config.connection_string) as conn:
    yield conn

# AFTER (compliant)
from declaro_persistum.pool import ConnectionPool

pool: ConnectionPool | None = None

async def init_pool(config: DatabaseConfig) -> None:
    global pool
    if config.dialect == "sqlite":
        pool = await ConnectionPool.sqlite(config.connection_string)
    elif config.dialect == "postgresql":
        pool = await ConnectionPool.postgresql(config.connection_string)
    elif config.dialect == "turso_cloud":
        pool = await ConnectionPool.turso(config.connection_string, auth_token=...)
    elif config.dialect == "turso_embedded":
        pool = ConnectionPool.turso_sync(config.connection_string)

@asynccontextmanager
async def get_connection():
    async with pool.acquire() as conn:
        yield conn
```

### Priority 2: Update Documentation

Add clarification to docs/usage.md that the Inspector examples show INTERNAL/ADVANCED usage, and typical application code should use ConnectionPool.

Add a "Best Practices" section explicitly stating:
- Application code MUST use ConnectionPool
- Direct driver imports are only for facade internals
- Inspector usage is an advanced pattern for tooling, not application data access

### Priority 3: Add Linting Rule

Consider adding a CI check that:
1. Scans non-persistum packages for direct driver imports
2. Fails the build if violations are detected
3. Allows exceptions via explicit comments (for edge cases)

---

## Files Confirmed Compliant

### declaro-tablix Source (No Direct DB Access)

All source files in declaro-tablix/src properly use:
- Protocol-based dependency injection
- `declaro_persistum.compat.SessionLocal` compatibility shim
- No direct database driver imports

### declaro-ximinez Source (No DB Code)

Pure static analysis tooling with no database operations.

### declaro-observe Source (No DB Code)

Event sourcing types only, no database operations.

### declaro-advise Source (Empty)

Only `__init__.py` exists, no implementations yet.

---

## Edge Cases and Clarifications

### Persistum Internal Tests

Tests in `packages/declaro-persistum/tests/` use direct driver connections. This is ACCEPTABLE because:
1. They test the facade itself
2. They need to verify driver-specific behavior
3. They are testing internals, not demonstrating application patterns

### Persistum CLI Commands

`cli/commands.py` has direct driver imports (lines 368-380). This is ACCEPTABLE because:
1. The CLI is part of the facade tooling
2. It needs direct access to verify connections during `declaro diff` etc.
3. It's infrastructure code, not application code

### Documentation Introspection Examples

The introspection examples in docs/usage.md showing direct connections are PARTIALLY ACCEPTABLE because:
1. Introspection is a low-level operation
2. However, documentation should clarify this is advanced/internal usage

---

## Implementation Plan Required

An implementation plan document should be created to address:

1. Refactor all 4 example applications to use ConnectionPool
2. Update docs/usage.md with Best Practices section
3. Add linting/CI check for direct driver imports outside persistum
4. Add facade usage validation tests

**Estimated effort**: 1-2 days for example refactoring, 0.5 days for documentation.

---

## Conclusion

The declaro monorepo architecture is sound - the facade pattern is correctly implemented in declaro-persistum and correctly consumed by declaro-tablix. However, the example applications within declaro-persistum itself demonstrate ANTI-PATTERNS that must be fixed to maintain architectural integrity and provide correct guidance to users.

The fix is straightforward: replace direct driver usage with ConnectionPool in the 4 example apps.
