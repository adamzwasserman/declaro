# Implementation Plan: Strangler Pattern Refactoring for Honest Code Compliance

---
**STATUS**: PARTIALLY COMPLETE
**VERSION**: 1.1
**DATE**: 2026-03-06
**PARENT**: 008-honest-code-audit-v1.md
**APPROACH**: Strangler Pattern -- one extraction per sprint, class becomes thin shell, thin shell eventually removed
---

## 1. Refactoring Strategy

The book prescribes the Strangler Pattern: "Extract one pure function from one class method per sprint. The method now calls the function. The class still exists; the interface doesn't change. After six months the class is a thin shell that does nothing, and removing it is a trivial cleanup."

Every step below preserves backward compatibility. The Protocol contracts do not change. The classes remain importable and functional throughout the migration. Tests continue to pass at every step.

---

## 2. Phase 1: Quick Wins (Week 1-2)

These are mechanical transformations with zero behavior change. Each can be done in a single commit.

### 2.1 Replace if/elif Factory Functions with Dict Dispatch ✅ COMPLETE

**Effort**: 30 minutes per factory. No behavior change.

**Target**: `applier/protocol.py`, `inspector/protocol.py`

```python
# Before (protocol.py:166-181)
def create_applier(dialect: str) -> MigrationApplier:
    if dialect == "postgresql":
        from declaro_persistum.applier.postgresql import PostgreSQLApplier
        return PostgreSQLApplier()
    elif dialect == "sqlite":
        from declaro_persistum.applier.sqlite import SQLiteApplier
        return SQLiteApplier()
    elif dialect == "turso":
        from declaro_persistum.applier.turso import TursoApplier
        return TursoApplier()
    else:
        raise ValueError(...)

# After
_APPLIER_FACTORIES: dict[str, Callable[[], MigrationApplier]] = {
    "postgresql": lambda: __import__(
        "declaro_persistum.applier.postgresql", fromlist=["PostgreSQLApplier"]
    ).PostgreSQLApplier(),
    "sqlite": lambda: __import__(
        "declaro_persistum.applier.sqlite", fromlist=["SQLiteApplier"]
    ).SQLiteApplier(),
    "turso": lambda: __import__(
        "declaro_persistum.applier.turso", fromlist=["TursoApplier"]
    ).TursoApplier(),
}

def create_applier(dialect: str) -> MigrationApplier:
    factory = _APPLIER_FACTORIES.get(dialect)
    if factory is None:
        supported = ", ".join(sorted(_APPLIER_FACTORIES))
        raise ValueError(f"Unsupported dialect: {dialect}. Supported: {supported}")
    return factory()
```

**Same pattern for**: `inspector/protocol.py:create_inspector()`, `check_compat.py:generate_validator()`, `reconstruction.py:get_reconstruction_columns()`.

**Risk**: None. Pure mechanical transformation.

**Test**: Run existing test suite. All tests pass with zero changes.

### 2.2 Extract `_normalize_fk_action()` to Shared Module ✅ COMPLETE

**Effort**: 15 minutes. The function is copy-pasted identically in `inspector/sqlite.py`, `inspector/postgresql.py`, `inspector/turso.py`.

**Action**:
1. Created `inspector/shared.py` (not `common.py` as originally proposed) with the shared function plus additional shared pure functions
2. Import from shared in all three inspector files
3. Deleted the duplicated definitions

**Risk**: None.

### 2.3 Replace query/executor.py if/elif Chains with Dict Dispatch

**Effort**: 1 hour. Five functions with identical dispatch pattern.

**Action**: Create a dispatch dict at module level:

```python
_FETCH_HANDLERS: dict[str, Callable] = {
    "asyncpg": _fetch_asyncpg,
    "aiosqlite": _fetch_aiosqlite,
    "libsql": _fetch_libsql,
}

def _detect_handler(connection: Any) -> Callable:
    conn_module = type(connection).__module__
    for key, handler in _FETCH_HANDLERS.items():
        if key in conn_module:
            return handler
    raise ValueError(f"Unsupported connection type: {conn_module}")
```

**Risk**: Low. The dispatch key matching must be tested.

---

## 3. Phase 2: Applier Strangler Extraction (Week 3-6)

This is the highest-impact refactoring. Goal: extract all pure SQL generation functions from the three applier classes into a shared module, leaving the classes as thin I/O shells.

### 3.1 Create `applier/shared.py` -- Shared Pure Functions ✅ COMPLETE

> **Note**: Created as `applier/shared.py` (not `sql_generators.py` as originally proposed).

**Effort**: 2-3 days.

Extracted the following pure functions from SQLiteApplier (they are identical or near-identical across sqlite and turso applier classes):

| Method on Class | Pure Function Signature | Dialect Variations |
|---|---|---|
| `_map_type(self, type_str)` | `map_type(type_str: str, dialect: str) -> str` | PostgreSQL has different type mappings |
| `_column_definition(self, name, definition)` | `column_definition(name: str, definition: Column, dialect: str) -> str` | PostgreSQL adds `SERIAL`, SQLite/Turso use `AUTOINCREMENT` |
| `_create_table_sql(self, table_name, columns, ...)` | `create_table_sql(table_name: str, columns: dict[str, Column], dialect: str) -> str` | Minor FK syntax differences |
| `_add_column_sql(self, table, column, definition)` | `add_column_sql(table: str, column: str, definition: Column, dialect: str) -> str` | Identical across dialects |
| `_drop_column_sql(self, table, column)` | `drop_column_sql(table: str, column: str) -> str` | Identical |
| `_alter_column_sql(self, table, column, changes)` | `alter_column_sql(table: str, column: str, changes: dict, dialect: str) -> list[str]` | PostgreSQL uses ALTER COLUMN, SQLite/Turso use reconstruction |
| `_add_index_sql(self, table, index, definition)` | `add_index_sql(table: str, index: str, definition: Index) -> str` | Nearly identical |
| `_drop_index_sql(self, table, index)` | `drop_index_sql(table: str, index: str) -> str` | Identical |
| `_add_fk_sql(self, table, column, references, ...)` | `add_fk_sql(table: str, column: str, references: str, dialect: str) -> list[str]` | PostgreSQL uses ALTER TABLE ADD CONSTRAINT, SQLite uses reconstruction |
| `_drop_fk_sql(self, table, column)` | `drop_fk_sql(table: str, column: str, dialect: str) -> list[str]` | Same difference |
| `generate_operation_sql(self, operation)` | `generate_operation_sql(operation: Operation, dialect: str) -> str` | Dispatcher; uses dict lookup internally already |

**Implementation Pattern**:

```python
# applier/sql_generators.py

from declaro_persistum.types import Column, Index, Operation

# Dict-lookup polymorphism for type mapping
_SQLITE_TYPE_MAP: dict[str, str] = {
    "uuid": "TEXT", "boolean": "INTEGER", "jsonb": "TEXT",
    "timestamptz": "TEXT", "varchar": "TEXT", ...
}

_POSTGRESQL_TYPE_MAP: dict[str, str] = {
    "integer": "INTEGER", "text": "TEXT", "boolean": "BOOLEAN",
    "uuid": "UUID", "jsonb": "JSONB", ...
}

_TYPE_MAPS: dict[str, dict[str, str]] = {
    "sqlite": _SQLITE_TYPE_MAP,
    "turso": _SQLITE_TYPE_MAP,  # Same as SQLite
    "postgresql": _POSTGRESQL_TYPE_MAP,
}

def map_type(type_str: str, dialect: str) -> str:
    """Map generic type to dialect-specific type. Pure function."""
    type_map = _TYPE_MAPS.get(dialect, _SQLITE_TYPE_MAP)
    return type_map.get(type_str.lower(), type_str.upper())


def column_definition(name: str, definition: Column, dialect: str) -> str:
    """Generate column definition SQL. Pure function."""
    ...


def generate_operation_sql(operation: Operation, dialect: str) -> str:
    """Generate SQL for a single operation. Pure function with dict dispatch."""
    generators = {
        "create_table": _gen_create_table,
        "drop_table": _gen_drop_table,
        "add_column": _gen_add_column,
        ...
    }
    generator = generators.get(operation["op"])
    if generator is None:
        raise ValueError(f"Unknown operation: {operation['op']}")
    return generator(operation, dialect)
```

**Strangler Step**: After extraction, each applier method becomes a one-liner:

```python
# applier/sqlite.py (after Strangler)
class SQLiteApplier:
    def generate_operation_sql(self, operation: Operation) -> str:
        return sql_generators.generate_operation_sql(operation, "sqlite")
```

**Risk**: Medium. Must verify SQL output is byte-for-byte identical for all dialects. Run full test suite including BDD tests.

**Rollback**: If any test fails, revert the extraction for that specific function. The Strangler Pattern allows partial extraction.

### 3.2 Create `inspector/shared.py` -- Shared Introspection Logic ✅ COMPLETE

> **Note**: Created as `inspector/shared.py` (not `introspection.py` as originally proposed).

**Effort**: 1-2 days.

Extracted shared logic between SQLiteInspector and TursoInspector:

- `_normalize_type()` (100% identical)
- `_introspect_table()` (~90% identical)
- `_get_columns()` (~85% identical -- Turso uses pragma_compat layer)
- `_get_unique_columns()` (~85% identical)
- `_get_indexes()` (~85% identical)
- `_get_foreign_keys()` (~80% identical)
- `_extract_view_query()` (100% identical, already module-level in both files)

**Pattern**: Create functions that take a `pragma_fn` parameter for the one point of variation (Turso uses pragma_compat, SQLite uses raw PRAGMA):

```python
# inspector/introspection.py

async def get_columns(
    connection: Any,
    table_name: str,
    *,
    pragma_table_info_fn: Callable = None,
    normalize_type_fn: Callable = normalize_sqlite_type,
) -> dict[str, Column]:
    """Get column definitions. Pure function with injected I/O."""
    if pragma_table_info_fn:
        rows = await pragma_table_info_fn(connection, table_name)
    else:
        cursor = await connection.execute(f"PRAGMA table_info('{table_name}')")
        rows = await cursor.fetchall()
    ...
```

**Risk**: Low. Inspector output is validated by existing introspection tests.

### 3.3 Deduplicate Async/Sync Execution Paths

**Effort**: 1 day.

Extract the shared logic from `apply()` and `apply_sync()` into a pure function that generates the execution plan, then have thin async/sync wrappers that execute it:

```python
# applier/execution.py

def build_execution_plan(
    operations: list[Operation],
    execution_order: list[int],
    dialect: str,
    *,
    dry_run: bool = False,
) -> list[str]:
    """Generate ordered list of SQL statements to execute. Pure function."""
    return [
        generate_operation_sql(operations[i], dialect)
        for i in execution_order
    ]


async def execute_plan_async(
    connection: Any,
    sql_statements: list[str],
    dialect: str,
) -> ApplyResult:
    """Execute SQL statements asynchronously. I/O boundary."""
    ...


def execute_plan_sync(
    connection: Any,
    sql_statements: list[str],
    dialect: str,
) -> ApplyResult:
    """Execute SQL statements synchronously. I/O boundary."""
    ...
```

**Risk**: Medium. Transaction handling differs slightly between backends. Must preserve rollback semantics.

---

## 4. Phase 3: Pool Refactoring (Week 7-8)

### 4.1 Replace ABC Hierarchy with Protocol + Composition — PARTIAL

**Effort**: 3-4 days.

> **UPDATE**: ABC removed from `BasePool` — it is now a plain class with `NotImplementedError` defaults. The full Protocol + composition refactoring proposed below was not done; the simpler approach of removing ABC was chosen instead.

Original proposal: Replace `ConnectionPool(ABC)` with a `ConnectionPool` Protocol and separate factory functions:

```python
# pool.py

class ConnectionPool(Protocol):
    """Protocol for connection pools. No inheritance."""

    async def acquire(self) -> AsyncContextManager[Any]: ...
    async def release(self, connection: Any) -> None: ...
    async def close(self) -> None: ...
    async def execute(self, sql: str, *args: Any) -> Any: ...
    async def fetch(self, sql: str, *args: Any) -> list[dict]: ...


async def create_pool(
    dialect: str,
    *,
    dsn: str = "",
    database_path: str = "",
    min_size: int = 1,
    max_size: int = 10,
) -> ConnectionPool:
    """Factory function. Configuration as parameters."""
    ...
```

**Risk**: High. pool.py is 61KB and used by external consumers (declaro-tablix). Must preserve the existing API surface.

**Mitigation**: Keep the old class-based API as deprecated aliases for one release cycle.

---

## 5. Phase 4: Module-Level State Cleanup (Week 9-10)

### 5.1 Replace Global Schema State with Explicit Parameters ✅ COMPLETE

**Effort**: 1 day.

In `query/table.py`, removed `set_default_schema()` and `load_default_schema()`. Made `table()` require explicit schema parameter:

```python
# Before
set_default_schema(my_schema)
users = table("users")  # uses global

# After
users = table("users", schema=my_schema)  # explicit
```

**Risk**: Low. The explicit parameter signature already exists; this just removes the default fallback.

### 5.2 Replace Module-Level Counters with Return Values

**Effort**: 2 days.

In `pragma_compat.py` and `check_compat.py`, replace module-level counters with return values:

```python
# Before
async def pragma_index_list(conn, table) -> list[tuple]:
    ...  # mutates _emulation_counters as side effect

# After
@dataclass(frozen=True)
class PragmaResult:
    rows: list[tuple]
    emulated: bool

async def pragma_index_list(conn, table) -> PragmaResult:
    ...  # returns whether emulation was used
```

**Risk**: Medium. Callers must be updated to unwrap `PragmaResult.rows`.

---

## 6. Phase 5: Final Cleanup (Week 11-12)

### 6.1 Remove Empty Class Shells

After all methods have been extracted to pure functions, the applier and inspector classes will be thin shells:

```python
class SQLiteApplier:
    def get_dialect(self) -> str:
        return "sqlite"

    def generate_operation_sql(self, operation: Operation) -> str:
        return sql_generators.generate_operation_sql(operation, "sqlite")

    async def apply(self, connection, operations, execution_order, *, dry_run=False):
        plan = execution.build_execution_plan(operations, execution_order, "sqlite", dry_run=dry_run)
        return await execution.execute_plan_async(connection, plan, "sqlite")
```

At this point, the classes are "a thin shell that does nothing" -- exactly what the Strangler Pattern predicts. They can be replaced with:

```python
def create_sqlite_applier() -> MigrationApplier:
    """Returns a MigrationApplier for SQLite."""
    return {
        "dialect": "sqlite",
        "generate_sql": partial(sql_generators.generate_operation_sql, dialect="sqlite"),
        "apply": partial(execution.execute_plan_async, dialect="sqlite"),
        "apply_sync": partial(execution.execute_plan_sync, dialect="sqlite"),
    }
```

Or they can remain as thin classes that satisfy the Protocol -- the book allows this for I/O boundaries.

### 6.2 Final Compliance Verification

Run the full test suite:
```bash
uv run pytest tests/unit/ -xvs
uv run pytest tests/bdd/ -xvs
uv run pytest -m precommit
```

Run type checking:
```bash
uv run mypy src/declaro_persistum/
```

Verify no regressions:
```bash
uv run ruff check src/
```

---

## 7. Milestone Definitions

| Milestone | Target Date | Deliverable | Success Criteria |
|---|---|---|---|
| M1: Quick Wins | Week 2 | Dict dispatch in all factories, shared `_normalize_fk_action` | All tests pass, zero if/elif chains in factory functions |
| M2: Applier Extraction | Week 6 | `applier/sql_generators.py` with all pure functions extracted | Applier classes are <50 lines each, all tests pass |
| M3: Inspector Dedup | Week 8 | `inspector/introspection.py` with shared logic | Inspector classes are <30 lines each, SQLite/Turso share code |
| M4: Async/Sync Dedup | Week 8 | `applier/execution.py` with shared execution logic | Zero duplicated apply/apply_sync logic |
| M5: Pool Refactoring | Week 10 | Protocol-based pool with factory function | ABC hierarchy removed, backward compat preserved |
| M6: State Cleanup | Week 12 | No module-level mutable state | All globals replaced with parameters or return values |

---

## 8. Risk Assessment

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| SQL generation regression | Medium | High | Byte-for-byte comparison of generated SQL before/after extraction |
| Pool API breaking change | Low | High | Keep deprecated aliases for one release cycle |
| Transaction semantics change | Medium | High | Integration tests with real databases for each dialect |
| Performance regression from function call overhead | Very Low | Low | Profile before/after; function dispatch is negligible vs I/O |
| Incomplete extraction leaves code in worse state | Medium | Medium | Each extraction is atomic -- either fully extracted or not started |

---

## 9. Testing Strategy

### Per-Extraction Tests

For each function extracted from a class to a module:

1. **Golden SQL Test**: Generate SQL with the old method and the new function for the same input. Assert byte-for-byte equality.
2. **Existing Unit Tests**: Must pass unchanged. If a test imports a class method, update the import but the assertion stays the same.
3. **BDD Tests**: Must pass unchanged. These test the system end-to-end.

### New Tests for Shared Modules

1. `test_sql_generators.py` -- Tests for every extracted pure function with all three dialect variations
2. `test_introspection.py` -- Tests for shared introspection logic with SQLite/Turso variations
3. `test_execution.py` -- Tests for execution plan building (pure) and execution (integration)

### Regression Prevention

After each phase, run the full precommit suite:
```bash
uv run pytest -m precommit --tb=short
```

---

## 10. Non-Goals

This plan explicitly does NOT propose:

1. **Changing the query/table.py class-based DSL**: The book acknowledges Python requires classes for operator overloading. TableProxy, ColumnProxy, Condition etc. are the honest minimum for enabling `users.email == "test@example.com"` syntax. These stay.

2. **Removing ConnectionPool classes entirely**: Connection pools manage inherently stateful resources. The book exempts this. The goal is to replace ABC inheritance with Protocol + composition, not to eliminate statefulness.

3. **Rewriting the entire codebase at once**: The Strangler Pattern is incremental. Each extraction is independently deployable and independently reversible.

4. **Breaking the MigrationApplier Protocol**: The Protocol contract is stable. Classes that implement it can be refactored internally without changing the protocol surface.
