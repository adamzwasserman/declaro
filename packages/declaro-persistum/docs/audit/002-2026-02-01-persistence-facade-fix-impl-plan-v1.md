# Implementation Plan: Persistence Facade Violation Fixes

**Document Version**: 1.0
**Date**: 2026-02-01
**Related Audit**: 001-2026-02-01-persistence-facade-audit-v1.md
**Status**: DRAFT

---

## Objective

Refactor the 4 example applications in declaro-persistum to use the ConnectionPool facade instead of direct database driver imports, eliminating architectural violations and providing correct reference implementations for users.

---

## Scope

### In Scope
- examples/todo_app_native/db.py
- examples/todo_app_django_style/db.py
- examples/todo_app_prisma_style/db.py
- examples/todo_app_sqlalchemy/db.py
- docs/usage.md (add Best Practices section)

### Out of Scope
- Persistum internal code (allowed to use drivers)
- Persistum tests (allowed to use drivers)
- CLI commands (infrastructure code, allowed)

---

## Prerequisites

1. Verify ConnectionPool supports all operations needed by example apps
2. Verify ConnectionPool has factory methods for all backends:
   - `ConnectionPool.sqlite()` - Verified
   - `ConnectionPool.postgresql()` - Verified
   - `ConnectionPool.turso()` (LibSQL cloud) - Verified
   - `ConnectionPool.turso_sync()` (pyturso embedded) - Verify exists

---

## Implementation Steps

### Step 1: Assess ConnectionPool API Completeness

**Goal**: Ensure ConnectionPool has all needed factory methods

**Actions**:
1. Review `/Users/adam/dev/declaro/packages/declaro-persistum/src/declaro_persistum/pool.py`
2. Document available factory methods
3. Identify any gaps for Turso embedded (pyturso) support

**Acceptance Criteria**:
- All 4 backends have ConnectionPool factory methods
- Factory methods return consistent connection interfaces

**Estimated Time**: 30 minutes

---

### Step 2: Create Shared Connection Module Template

**Goal**: Design the refactored db.py structure

**Actions**:
1. Design a connection module that:
   - Uses ConnectionPool exclusively
   - Provides the same external interface (`get_connection()`)
   - Supports all 4 dialects
   - Maintains backward compatibility with app.py files

2. Draft the template:

```python
"""
Database connection module - uses declaro-persistum facade.

This module abstracts database connections via ConnectionPool,
providing a unified interface across SQLite, PostgreSQL, and Turso backends.
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator, Any

from declaro_persistum.pool import ConnectionPool

# Global pool reference
_pool: ConnectionPool | None = None


@dataclass(frozen=True)
class DatabaseConfig:
    """Immutable database configuration."""
    dialect: str  # "sqlite", "postgresql", "turso_cloud", or "turso_embedded"
    connection_string: str
    display_name: str
    auth_token: str | None = None


# Factory functions remain unchanged
def get_sqlite_config(database_path: str | None = None) -> DatabaseConfig:
    path = database_path or str(Path(__file__).parent / "todos.db")
    return DatabaseConfig(
        dialect="sqlite",
        connection_string=path,
        display_name=f"SQLite ({Path(path).name})"
    )


# ... other config factories unchanged ...


async def init_pool(config: DatabaseConfig) -> None:
    """Initialize the global connection pool."""
    global _pool

    if config.dialect == "sqlite":
        _pool = await ConnectionPool.sqlite(config.connection_string)
    elif config.dialect == "postgresql":
        _pool = await ConnectionPool.postgresql(config.connection_string)
    elif config.dialect == "turso_cloud":
        _pool = await ConnectionPool.turso(
            config.connection_string,
            auth_token=config.auth_token or ""
        )
    elif config.dialect == "turso_embedded":
        _pool = ConnectionPool.turso_sync(config.connection_string)
    else:
        raise ValueError(f"Unknown dialect: {config.dialect}")


async def close_pool() -> None:
    """Close the global connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def get_connection() -> AsyncGenerator[Any, None]:
    """
    Get a database connection from the pool.

    Usage:
        async with get_connection() as conn:
            results = await query.execute(conn)
    """
    if _pool is None:
        raise RuntimeError("Pool not initialized. Call init_pool() first.")

    async with _pool.acquire() as conn:
        yield conn


async def test_connection(config: DatabaseConfig) -> tuple[bool, str]:
    """Test if a database connection works."""
    try:
        await init_pool(config)
        async with get_connection() as conn:
            # Pool handles dialect-specific "SELECT 1" execution
            pass
        return True, "Connection successful"
    except ImportError as e:
        return False, f"Driver not installed: {e}"
    except Exception as e:
        return False, f"Connection failed: {e}"
    finally:
        await close_pool()


async def init_schema(config: DatabaseConfig) -> None:
    """Initialize the database schema using declaro-persistum."""
    from declaro_persistum.query import execute, raw

    await init_pool(config)

    # Use raw SQL via facade
    if config.dialect in ("sqlite", "turso_cloud", "turso_embedded"):
        sql = """
            CREATE TABLE IF NOT EXISTS todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """
    elif config.dialect == "postgresql":
        sql = """
            CREATE TABLE IF NOT EXISTS todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """
    else:
        raise ValueError(f"Unknown dialect: {config.dialect}")

    async with get_connection() as conn:
        await execute(raw(sql), conn)
        if hasattr(conn, 'commit'):
            await conn.commit()
```

**Acceptance Criteria**:
- Template compiles without errors
- Template maintains same external interface
- No direct driver imports

**Estimated Time**: 1 hour

---

### Step 3: Update PostgreSQL Database Creation

**Goal**: Handle PostgreSQL database creation through facade

**Challenge**: The current code creates databases by connecting to `postgres` database first. This is an administrative operation.

**Options**:
1. Keep direct asyncpg for admin operations (documented exception)
2. Add admin connection support to ConnectionPool
3. Document that PostgreSQL database must exist before use

**Recommendation**: Option 3 - Document that database must exist. Database creation is a deployment/infrastructure concern, not application concern.

**Actions**:
1. Remove `ensure_postgres_database()` function
2. Add documentation that database must be pre-created
3. Update test_connection to only test, not create

**Estimated Time**: 30 minutes

---

### Step 4: Implement Example App Refactoring

**Goal**: Apply the template to all 4 example apps

**Actions**:
1. Refactor todo_app_native/db.py
2. Verify app.py works unchanged
3. Run example app tests
4. Repeat for remaining 3 apps

**Per-App Checklist**:
- [ ] Replace driver imports with ConnectionPool
- [ ] Update init_pool() for facade pattern
- [ ] Update get_connection() to use pool.acquire()
- [ ] Update test_connection() to use facade
- [ ] Remove ensure_postgres_database() (document requirement)
- [ ] Remove check_driver_available() (ConnectionPool handles this)
- [ ] Test all 4 dialects
- [ ] Verify existing app.py works unchanged

**Estimated Time**: 2 hours (30 min per app)

---

### Step 5: Update Documentation

**Goal**: Add Best Practices section to docs/usage.md

**Actions**:
1. Add new section "Connection Best Practices"
2. Clarify that application code should use ConnectionPool
3. Explain Inspector examples are for advanced/internal use
4. Add example of correct application connection pattern

**Content to Add**:

```markdown
## Connection Best Practices

### For Application Code

Application code should ALWAYS use `ConnectionPool` for database access:

```python
from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query import execute, raw

# Initialize once at startup
pool = await ConnectionPool.sqlite("./app.db")

# Acquire connections as needed
async with pool.acquire() as conn:
    result = await execute(raw("SELECT * FROM users"), conn)
```

### For Advanced/Internal Use

The Inspector examples in this documentation show direct driver usage for
database introspection. This pattern is for:
- CLI tooling
- Migration tools
- Schema analysis

Application data access should NOT use this pattern.
```

**Estimated Time**: 30 minutes

---

### Step 6: Add CI Validation (Optional)

**Goal**: Prevent future violations

**Actions**:
1. Create a simple grep-based check script
2. Add to pre-commit or CI pipeline
3. Allow documented exceptions

**Script Example**:
```bash
#!/bin/bash
# check_facade_violations.sh

# Directories to check (application code, not internals)
DIRS=(
    "packages/declaro-tablix/src"
    "packages/declaro-tablix/examples"
    "packages/declaro-advise/src"
    "packages/declaro-observe/src"
)

# Pattern to detect
PATTERN="import aiosqlite|import asyncpg|import libsql|import turso|from aiosqlite|from asyncpg"

VIOLATIONS=0
for dir in "${DIRS[@]}"; do
    if grep -rE "$PATTERN" "$dir" --include="*.py" 2>/dev/null; then
        echo "VIOLATION: Direct driver import in $dir"
        VIOLATIONS=$((VIOLATIONS + 1))
    fi
done

exit $VIOLATIONS
```

**Estimated Time**: 1 hour

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| ConnectionPool missing Turso embedded support | Medium | High | Step 1 verification |
| Breaking existing app.py files | Low | Medium | Maintain same interface |
| Missing edge cases in examples | Medium | Low | Manual testing |

---

## Rollback Plan

If issues are discovered:
1. Revert db.py changes via git
2. Document issues found
3. Address ConnectionPool gaps before retrying

---

## Success Criteria

1. All 4 example apps work with all 4 backends
2. No direct driver imports in example apps
3. Example apps demonstrate correct facade usage
4. Documentation clearly states best practices
5. CI check passes (if implemented)

---

## Timeline

| Step | Task | Estimate | Dependencies |
|------|------|----------|--------------|
| 1 | Assess ConnectionPool | 30 min | None |
| 2 | Create template | 1 hour | Step 1 |
| 3 | Handle PostgreSQL | 30 min | Step 2 |
| 4 | Refactor 4 apps | 2 hours | Steps 2, 3 |
| 5 | Update docs | 30 min | Step 4 |
| 6 | CI check (optional) | 1 hour | Step 4 |

**Total Estimated Time**: 5.5 hours (4.5 hours without CI check)

---

## Appendix: Files to Modify

### Primary Files
- `/Users/adam/dev/declaro/packages/declaro-persistum/examples/todo_app_native/db.py`
- `/Users/adam/dev/declaro/packages/declaro-persistum/examples/todo_app_django_style/db.py`
- `/Users/adam/dev/declaro/packages/declaro-persistum/examples/todo_app_prisma_style/db.py`
- `/Users/adam/dev/declaro/packages/declaro-persistum/examples/todo_app_sqlalchemy/db.py`
- `/Users/adam/dev/declaro/packages/declaro-persistum/docs/usage.md`

### No Changes Needed
- `/Users/adam/dev/declaro/packages/declaro-tablix/examples/demo/app.py` - Already compliant
- `/Users/adam/dev/declaro/packages/declaro-persistum/src/` - Internal (allowed)
- `/Users/adam/dev/declaro/packages/declaro-persistum/tests/` - Testing (allowed)
