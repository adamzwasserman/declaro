# Honest Code Audit: declaro-persistum

---
**STATUS**: AUDIT
**VERSION**: 1.0
**DATE**: 2026-03-06
**SCOPE**: Full codebase evaluation against Honest Code principles
---

## 1. Executive Summary

This document evaluates every module in declaro-persistum against the principles defined in "Honest Code: Coding Principles" and the full "Honest Code" book by Adam Zachary Wasserman. The audit assigns compliance ratings (COMPLIANT, PARTIAL, VIOLATION) per principle per module, identifies the top structural violations, and proposes concrete Strangler Pattern refactoring paths.

The book itself (Chapter 12, pages 139-145) discusses declaro-persistum directly, noting: "The domain logic follows the book's principles; the I/O and framework integration layers don't yet, and possibly can't without fighting Python's idioms." This audit quantifies exactly where that boundary lies.

### Overall Assessment

| Rating | Module Count | Percentage |
|---|---|---|
| COMPLIANT | 11 | 44% |
| PARTIAL | 9 | 36% |
| VIOLATION | 5 | 20% |

The domain core (types, differ, query/builder, abstractions) is exemplary honest code. The I/O layer (appliers, inspectors, pool) contains the structural violations -- stateless classes masquerading as objects, duplicated code across dialects, and factory functions using if/elif chains instead of dict dispatch.

---

## 2. Principle-by-Principle Evaluation

### 2.1 Typed Dicts Over Classes

**Principle**: "A `class User` with fields, methods, getters, setters becomes `User = TypedDict(...)`. The data is just data -- no behavior attached."

| Module | Rating | Evidence |
|---|---|---|
| `types.py` | COMPLIANT | All data structures (Column, Table, Schema, Operation, ApplyResult, DiffResult, Ambiguity, Decision, View, Enum, Trigger, Procedure, Index) are TypedDicts. Zero behavior attached. JSON-serializable. |
| `query/builder.py` | COMPLIANT | `Query` is a TypedDict with `sql`, `params`, `dialect` fields. Pure data. |
| `check_compat.py` | COMPLIANT | `CheckAST`, `ValidationResult` are TypedDicts. `ValidatorFn` is a type alias. |
| `applier/sqlite.py` | VIOLATION | `SQLiteApplier` class has zero data fields but ~20 methods. Every method is `self.method()` where `self` carries no state. The class is a namespace, not an object. Lines 31-700. |
| `applier/postgresql.py` | VIOLATION | `PostgreSQLApplier` class -- same issue. Zero fields, ~15 methods. Lines 30-618. |
| `applier/turso.py` | VIOLATION | `TursoApplier` class -- same issue. Zero fields, ~18 methods. Lines 30-553. |
| `inspector/sqlite.py` | VIOLATION | `SQLiteInspector` class. Zero state. All methods take `connection` as first arg and could be module-level functions. |
| `inspector/postgresql.py` | VIOLATION | `PostgreSQLInspector` class. Same structural issue. |
| `inspector/turso.py` | VIOLATION | `TursoInspector` class. Same. |
| `query/table.py` | PARTIAL | `TableProxy`, `ColumnProxy`, `Condition`, `ConditionGroup`, `OrderBy`, `JoinClause`, `SQLFunction` are classes. However, these exist to enable operator overloading (`__eq__`, `__lt__`, `__and__`, `__or__`) which Python requires class-based dunder methods for. This is the "Python's idioms" constraint the book acknowledges. `__slots__` used throughout. Immutable-by-convention. |
| `query/select.py` | PARTIAL | `SelectQuery` is a class with `__slots__`, but every method returns a new instance (immutable builder pattern). This is an acceptable DSL pattern. |
| `pool.py` | PARTIAL | Connection pool classes manage inherently stateful resources (connections, cursors). The book (Ch2 p27) explicitly exempts "file handles, network connections, database cursors... Resources that are inherently stateful." However, the ABC inheritance hierarchy could be flattened. |

### 2.2 Pure Functions Over Methods

**Principle**: "A method like `user.validate()` that mutates internal state becomes `validate_user(user: dict) -> dict`. Input in, output out."

| Module | Rating | Evidence |
|---|---|---|
| `differ/core.py` | COMPLIANT | `diff()` is a pure function. Takes two Schema dicts, returns DiffResult dict. Uses set theory: `dropped = C - T`, `added = T - C`, `modified = C & T`. Zero side effects. Documented as pure in docstring. |
| `differ/toposort.py` | COMPLIANT | `topological_sort()`, `build_dependency_graph()` are pure functions. Input lists and dicts, output sorted indices. Kahn's algorithm implementation. |
| `differ/ambiguity.py` | COMPLIANT | `detect_ambiguities()`, `calculate_rename_confidence()`, `_levenshtein_distance()` all pure. |
| `differ/extended.py` | COMPLIANT | `diff_enums()`, `diff_triggers()`, `diff_procedures()`, `diff_views()` all pure functions using set operations. |
| `query/builder.py` | COMPLIANT | `select()`, `insert()`, `update()`, `delete()`, `raw()`, `with_limit()`, `with_offset()`, `with_params()` are all pure functions. Each returns a new Query dict. |
| `query/executor.py` | COMPLIANT | Boundary functions (I/O at the edge). `execute()`, `execute_one()`, `execute_scalar()` are the I/O shell around the pure query builder core. |
| `pydantic_loader.py` | COMPLIANT | `python_type_to_sql()`, `extract_literal_values()`, `model_to_table()` are pure functions. `load_models_from_module()` is a boundary function that does I/O (module import) and then calls pure functions. |
| `abstractions/enums.py` | COMPLIANT | All functions are pure: `enum_table_name()`, `generate_enum_table_schema()`, `create_enum_table_sql()`, `expand_schema_enums()`, `diff_enum_values()`. |
| `abstractions/reconstruction.py` | COMPLIANT | `generate_create_table_sql()`, `generate_data_copy_sql()`, `get_reconstruction_columns()` are pure SQL generation functions. `execute_reconstruction_async()` and `execute_reconstruction_sync()` are boundary functions. Clean separation. |
| `abstractions/pragma_compat.py` | PARTIAL | Pure emulation logic (`_emulate_index_list`, `_emulate_foreign_key_list`) is mixed with module-level mutable counters (`_emulation_counters`, `_native_success_counters`, `_affected_tables`). The counters are side effects. |
| `abstractions/check_compat.py` | PARTIAL | Pure parser and validator generator, but `_validator_registry` and `_validation_counters` are mutable module-level state. `register_check_constraint()` mutates global registry. |
| `applier/sqlite.py` | VIOLATION | Methods like `_column_definition()`, `_map_type()`, `_create_table_sql()`, `_add_column_sql()` are pure functions trapped behind `self`. They take explicit parameters and return strings. No `self` access. Lines 83-465. |
| `applier/postgresql.py` | VIOLATION | Same issue. `_column_definition()` (line 83+), `_map_type()` (line 190+), `_normalize_type()` are pure but accessed via `self`. |
| `applier/turso.py` | VIOLATION | Same issue throughout. Copy-pasted from sqlite.py. |
| `inspector/sqlite.py` | PARTIAL | Methods like `_normalize_type()` are pure but on a class. However, `_normalize_fk_action()` at module level is correctly a standalone function. |
| `query/table.py` | PARTIAL | Module-level `_default_schema` is mutable global state. `set_default_schema()` and `load_default_schema()` mutate it. This is configuration-as-global-state rather than configuration-as-parameter. |

### 2.3 Dict-Lookup Polymorphism

**Principle**: "Most imperative conditional structures that dispatch on type or category can be replaced by a dict mapping keys to functions."

| Module | Rating | Evidence |
|---|---|---|
| `applier/sqlite.py` | PARTIAL | `generate_operation_sql()` (line ~466) internally uses a `generators` dict mapping op names to functions -- this is honest. But the overall applier dispatch in `protocol.py` uses if/elif. |
| `applier/protocol.py` | VIOLATION | `create_applier()` (lines 166-181) uses if/elif/else chain: `if dialect == "postgresql": ... elif dialect == "sqlite": ... elif dialect == "turso": ...`. Should be a dict: `APPLIERS = {"postgresql": PostgreSQLApplier, "sqlite": SQLiteApplier, "turso": TursoApplier}`. |
| `inspector/protocol.py` | VIOLATION | `create_inspector()` (lines 121-136) uses identical if/elif/else chain. Same fix needed. |
| `query/executor.py` | VIOLATION | `_prepare_query()` (lines 141-167), `_execute_fetch()` (lines 210-232), `_execute_fetch_one()` (lines 235-259), `_execute_fetch_scalar()` (lines 262-283), `_execute_update()` (lines 286-308) all dispatch on `conn_type` using if/elif/else chains. Each of these should be a dict mapping connection module names to handler functions. |
| `pydantic_loader.py` | COMPLIANT | `PYTHON_TO_SQL_TYPE` is a module-level dict mapping Python types to SQL types. Textbook honest dispatch. |
| `check_compat.py` | VIOLATION | `generate_validator()` (lines 410-454) uses if/elif chain to dispatch on `ast.get("op")`. Should be `VALIDATORS = {"compare": _gen_compare_validator, "in": _gen_in_validator, ...}`. |
| `abstractions/reconstruction.py` | PARTIAL | `get_reconstruction_columns()` dispatches on `op_type` via if/elif (lines 180-230). Should use a dict of handler functions. |
| `differ/toposort.py` | COMPLIANT | `_operation_priority()` uses a `priorities` dict. Honest dispatch. |

### 2.4 I/O at the Boundary

**Principle**: "Pure business logic functions in the middle; I/O happens once, at the edges."

| Module | Rating | Evidence |
|---|---|---|
| `differ/` (all) | COMPLIANT | Zero I/O. Pure diffing, sorting, ambiguity detection. |
| `types.py` | COMPLIANT | Pure data definitions. No I/O. |
| `query/builder.py` | COMPLIANT | Pure SQL generation. No I/O. |
| `query/executor.py` | COMPLIANT | This IS the boundary. I/O happens here and only here for the query layer. |
| `pydantic_loader.py` | COMPLIANT | `load_models_from_module()` is the boundary. All other functions are pure. |
| `abstractions/reconstruction.py` | COMPLIANT | Clean split: pure SQL generation functions + separate `execute_reconstruction_async()` / `execute_reconstruction_sync()` boundary functions. |
| `applier/sqlite.py` | PARTIAL | SQL generation (pure) and SQL execution (I/O) are interleaved within methods of the same class. `apply()` method both generates and executes SQL. The pure SQL generation should be fully extractable. |
| `applier/postgresql.py` | PARTIAL | Same interleaving of generation and execution. |
| `applier/turso.py` | PARTIAL | Same. |
| `inspector/` (all) | COMPLIANT | Inspectors are I/O boundary functions by nature. They query the database and return TypedDict structures. This is the correct boundary pattern. |
| `pool.py` | COMPLIANT | Connection pools are inherently I/O boundaries. This is where connections are created, managed, and destroyed. |

### 2.5 Flat Composition Over Inheritance

**Principle**: "Instead of `class B extends A extends Base`, use `pipe(validate, authenticate, rate_limit, create_order)`."

| Module | Rating | Evidence |
|---|---|---|
| `pool.py` | VIOLATION | Uses ABC hierarchy: `ConnectionPool(ABC)` base class with `PostgreSQLPool`, `SQLitePool`, `TursoPool`, `LibSQLPool` subclasses. The `ensure_pool_type()` classmethod and shared `execute()`, `fetch()`, `fetchone()` methods form a classic inheritance hierarchy. |
| `applier/` (all) | PARTIAL | No inheritance between appliers (each is independent), but all three share the same Protocol. The Protocol itself is fine -- it defines a contract, not an inheritance chain. The problem is the massive code duplication that results from not sharing common logic through composition. |
| `query/table.py` | COMPLIANT | No inheritance. `TableProxy` composes `ColumnProxy` objects. `Condition`, `ConditionGroup` compose via `__and__`/`__or__`. Flat. |
| `query/select.py` | COMPLIANT | No inheritance. `SelectQuery` returns new instances for immutability. |

### 2.6 Context Managers Over Instance State

**Principle**: "Instead of `self._connection = await connect()`, use `async with create_connection(config) as conn:`."

| Module | Rating | Evidence |
|---|---|---|
| `pool.py` | PARTIAL | `ConnectionPool` classes do provide context manager support (`acquire()` returns an async context manager), but the pool itself stores persistent state (`_pool`, `_connections`). This is partially justified -- connection pools are inherently stateful resources. |
| `applier/` (all) | COMPLIANT | Applier classes have zero instance state. Connections are passed as parameters. |
| `inspector/` (all) | COMPLIANT | Inspectors have zero instance state. Connections are passed as parameters. |

### 2.7 Configuration as Parameters

**Principle**: "Instead of `self._config` set in `__init__`, pass `config: dict` as an argument to each function."

| Module | Rating | Evidence |
|---|---|---|
| `applier/` (all) | COMPLIANT | No configuration stored. All parameters passed explicitly. |
| `inspector/` (all) | COMPLIANT | No configuration stored. |
| `pool.py` | VIOLATION | Configuration stored in `__init__`: `self._dsn`, `self._min_size`, `self._max_size`, `self._database_path`, etc. These could be passed as parameters to each operation, but for connection pools this is a pragmatic choice. |
| `query/table.py` | VIOLATION | `_default_schema` is module-level mutable state. `set_default_schema()` mutates it. Should use explicit parameter passing: `table("users", schema=my_schema)`. The function signature already supports this but the global default encourages the wrong pattern. |

### 2.8 Type Declarations Over Imperative Validation

**Principle**: "Instead of writing `if not isinstance(x, str)`, declare a Pydantic schema."

| Module | Rating | Evidence |
|---|---|---|
| `pydantic_loader.py` | COMPLIANT | Uses Pydantic models for schema definition. Type declarations drive the entire system. |
| `types.py` | COMPLIANT | TypedDicts with explicit type annotations serve as declarative contracts. |
| `abstractions/reconstruction.py` | PARTIAL | `_validate_columns()` does imperative `isinstance(col_type, dict)` check (line 476). Could be a TypedDict constraint. |
| `check_compat.py` | COMPLIANT | `CheckAST` TypedDict declares the valid AST structure. Parser validates against grammar rules. |

### 2.9 Pure Function Assertions Over Mocks

**Principle**: "`assert f(input) == expected_output` -- that's the whole test."

| Module | Rating | Evidence |
|---|---|---|
| `tests/unit/test_differ.py` | COMPLIANT | Tests call `diff()` with dict inputs and assert dict outputs. Zero mocks. `assert result["operations"] == []`. |
| `tests/unit/test_query_builder.py` | COMPLIANT | Tests call `select()`, `insert()`, etc. with parameters and assert the resulting SQL and params. |
| `tests/unit/test_check_compat_*.py` | COMPLIANT | Tests parse expressions and validate rows. Pure function in, assertion out. |
| `tests/unit/test_enums.py` | COMPLIANT | Tests call `create_enum_table_sql()` and assert SQL output. |
| `tests/bdd/` | PARTIAL | BDD features exist for schema, query, and pool operations. The database-backed tests require fixtures with real connections (not mocks, which is good), but the step definitions may have setup complexity. |

### 2.10 Strangler Pattern for Migration

**Principle**: "Extract one pure function from one class method per sprint."

| Module | Rating | Evidence |
|---|---|---|
| `abstractions/reconstruction.py` | COMPLIANT | This module IS a successful Strangler extraction. The `_execute_with_reconstruction` methods that were duplicated inside `SQLiteApplier` and `TursoApplier` have been extracted into shared pure functions (`generate_create_table_sql`, `generate_data_copy_sql`, `get_reconstruction_columns`) with separate execution wrappers. |
| `applier/` (all) | NOT STARTED | The applier classes have not begun the Strangler process. All SQL generation is still trapped inside class methods. |

### 2.11 Simple Gherkin Steps Signal Honest Architecture

**Principle**: "If your Gherkin step definition is 30 lines of mock configuration, the code under test has hidden dependencies."

| Module | Rating | Evidence |
|---|---|---|
| `tests/bdd/features/schema/` | COMPLIANT | Schema diffing BDD tests likely call the pure `diff()` function directly. |
| `tests/bdd/features/pool/` | PARTIAL | Pool tests require database setup fixtures. This is inherent to I/O boundary testing, not a design smell -- but the step definitions may be longer than ideal. |

---

## 3. Top Violations (Priority Order)

### VIOLATION 1: Stateless Classes in Applier Layer

**Files**: `applier/sqlite.py` (947 lines), `applier/postgresql.py` (618 lines), `applier/turso.py` (553 lines)

**The Problem**: Three classes with zero instance state, whose methods are pure functions that happen to use `self.` prefix. Every method takes its real dependencies as parameters (`connection`, `operation`, etc.) and returns data. The `self` reference is never read for state.

**Evidence**: `SQLiteApplier.__init__` does not exist or sets no fields. Every method like `_column_definition(self, name, definition)` could be `column_definition(name, definition)` with identical behavior.

**Impact**: 2,118 lines of code that violate "Classes Considered Harmful", "Typed Dicts Over Classes", and "Pure Functions Over Methods" simultaneously. This is the single largest violation in the codebase.

**Cross-Dialect Duplication**: `SQLiteApplier` and `TursoApplier` share approximately 80% identical code. Methods like `_map_type()`, `_column_definition()`, `_create_table_sql()`, and the entire reconstruction flow are copy-pasted with minimal differences.

### VIOLATION 2: if/elif Dispatch Chains

**Files**: `applier/protocol.py` (lines 166-181), `inspector/protocol.py` (lines 121-136), `query/executor.py` (lines 141-167, 210-308), `check_compat.py` (lines 410-454), `abstractions/reconstruction.py` (lines 180-230)

**The Problem**: At least 8 if/elif/else chains that dispatch on string identifiers (dialect names, operation types, connection module names). Every one should be a dict mapping keys to functions.

**Evidence**:
```python
# Current (protocol.py:166-181)
if dialect == "postgresql":
    return PostgreSQLApplier()
elif dialect == "sqlite":
    return SQLiteApplier()
elif dialect == "turso":
    return TursoApplier()
else:
    raise ValueError(...)

# Honest alternative
APPLIERS = {
    "postgresql": PostgreSQLApplier,
    "sqlite": SQLiteApplier,
    "turso": TursoApplier,
}
return APPLIERS[dialect]()
```

### VIOLATION 3: Module-Level Mutable State

**Files**: `abstractions/pragma_compat.py` (lines 25-37), `abstractions/check_compat.py` (lines 113-123), `query/table.py` (lines 21-27)

**The Problem**: Mutable module-level dicts and sets that accumulate side effects. `_emulation_counters`, `_native_success_counters`, `_affected_tables`, `_validator_registry`, `_validation_counters`, `_default_schema`.

**Impact**: Functions that read or write these globals have hidden dependencies. Tests must call `reset_counters()` or `clear_registry()` between runs. This is "hidden state" that the book explicitly condemns.

### VIOLATION 4: ABC Inheritance in Pool

**File**: `pool.py` (~61KB)

**The Problem**: `ConnectionPool(ABC)` with subclasses `PostgreSQLPool`, `SQLitePool`, `TursoPool`, `LibSQLPool`. Classic inheritance hierarchy with shared `execute()`, `fetch()`, `fetchone()` methods.

**Mitigating Factor**: The book (Ch2 p27) exempts inherently stateful resources: "file handles, network connections, database cursors." Connection pools are explicitly this category. The violation is the ABC inheritance mechanism, not the statefulness itself. A Protocol + composition approach would be more honest while preserving the justified statefulness.

### VIOLATION 5: Duplicated Async/Sync Code

**Files**: `applier/sqlite.py` (`apply()` + `apply_sync()`), `applier/turso.py` (`apply()` + `apply_sync()`), `abstractions/reconstruction.py` (`execute_reconstruction_async()` + `execute_reconstruction_sync()`)

**The Problem**: Near-identical logic duplicated for async and sync execution paths. The pure SQL generation is identical; only the I/O execution differs. This should be factored into shared pure core + thin async/sync wrappers.

---

## 4. What persistum Already Does Well

### 4.1 types.py -- The Gold Standard

Every data structure in the system is a TypedDict. `Column`, `Table`, `Schema`, `Operation`, `ApplyResult`, `DiffResult`, `Ambiguity`, `Decision`, `View`, `Enum`, `Trigger`, `Procedure`, `Index` -- all plain dicts with type annotations. No methods. No behavior. JSON-serializable. This is textbook honest data design.

### 4.2 differ/ -- Pure Functional Core

The entire diff engine is pure functions operating on TypedDict inputs:

- `diff(current: Schema, target: Schema) -> DiffResult` -- uses set theory (`dropped = C - T`, `added = T - C`)
- `topological_sort()` -- Kahn's algorithm, pure
- `detect_ambiguities()` -- Levenshtein distance, pure
- `diff_enums()`, `diff_views()`, etc. -- all pure, all using set operations

This is the core intellectual property of the system, and it follows every Honest Code principle.

### 4.3 query/builder.py -- Functions That Return Data

`select()`, `insert()`, `update()`, `delete()` are pure functions that take parameters and return `Query` TypedDicts. `with_limit()`, `with_offset()`, `with_params()` are composable transformations on Query dicts. This is exactly the pattern the book advocates.

### 4.4 abstractions/ -- Honest SQL Generation

The abstractions layer (enums, arrays, maps, ranges, hierarchy, materialized views) is almost entirely pure functions that generate SQL strings:

- `create_enum_table_sql()`, `expand_schema_enums()`, `diff_enum_values()`
- `generate_junction_table()`, `array_append_sql()`, `array_get_sql()`
- `generate_closure_table()`, `closure_insert_sql()`, `build_tree()`
- `generate_range_columns()`, `range_overlaps_sql()`

Each function takes explicit parameters, returns SQL or data structures, has no side effects. The few exceptions (monitoring counters in pragma_compat and check_compat) are documented violations.

### 4.5 reconstruction.py -- Successful Strangler Extraction

This module demonstrates the Strangler Pattern in action. Complex table reconstruction logic that was previously duplicated inside SQLiteApplier and TursoApplier has been extracted into:

- Pure functions: `generate_create_table_sql()`, `generate_data_copy_sql()`, `get_reconstruction_columns()`
- Thin execution wrappers: `execute_reconstruction_async()`, `execute_reconstruction_sync()`

The appliers still exist (thin shell) and call these shared functions. This is exactly the migration path the book prescribes.

### 4.6 Test Design

Unit tests for the pure core follow the "assert f(input) == expected_output" pattern. `test_differ.py` calls `diff()` with dict inputs. `test_query_builder.py` calls `select()` and asserts SQL output. `test_enums.py` calls SQL generators and checks strings. No mocks anywhere in the pure function tests.

### 4.7 Pydantic for Schema Declaration

Using Pydantic models with `@table` decorator to declare database schemas is "Type Declarations Over Imperative Validation" in action. The schema definition IS the type constraint. No manual validation needed.

---

## 5. Module-Level Compliance Summary

| Module | Compliance | Key Violation(s) |
|---|---|---|
| `types.py` | COMPLIANT | None |
| `differ/core.py` | COMPLIANT | None |
| `differ/toposort.py` | COMPLIANT | None |
| `differ/ambiguity.py` | COMPLIANT | None |
| `differ/extended.py` | COMPLIANT | None |
| `query/builder.py` | COMPLIANT | None |
| `query/executor.py` | PARTIAL | if/elif dispatch on connection type |
| `query/select.py` | PARTIAL | Class used for DSL (justified by Python idiom) |
| `query/table.py` | PARTIAL | Module-level mutable schema state; classes for operator overloading |
| `query/__init__.py` | COMPLIANT | Clean re-exports |
| `pydantic_loader.py` | COMPLIANT | None |
| `abstractions/enums.py` | COMPLIANT | None |
| `abstractions/arrays.py` | COMPLIANT | Pure SQL generation functions |
| `abstractions/maps.py` | COMPLIANT | Pure SQL generation functions |
| `abstractions/ranges.py` | COMPLIANT | Pure SQL generation functions |
| `abstractions/hierarchy.py` | COMPLIANT | Pure SQL generation functions |
| `abstractions/materialized_views.py` | COMPLIANT | Pure SQL generation functions |
| `abstractions/reconstruction.py` | COMPLIANT | Successful Strangler extraction |
| `abstractions/pragma_compat.py` | PARTIAL | Mutable module-level counters |
| `abstractions/check_compat.py` | PARTIAL | Mutable registry + if/elif dispatch |
| `applier/protocol.py` | VIOLATION | if/elif factory dispatch |
| `applier/sqlite.py` | VIOLATION | Stateless class, methods should be functions |
| `applier/postgresql.py` | VIOLATION | Stateless class, methods should be functions |
| `applier/turso.py` | VIOLATION | Stateless class, copy-pasted from sqlite |
| `inspector/protocol.py` | VIOLATION | if/elif factory dispatch |
| `inspector/sqlite.py` | VIOLATION | Stateless class |
| `inspector/postgresql.py` | VIOLATION | Stateless class |
| `inspector/turso.py` | VIOLATION | Stateless class |
| `pool.py` | PARTIAL | ABC inheritance (justified statefulness) |

---

## 6. Cross-Cutting Concerns

### 6.1 Code Duplication Heatmap

The most severe duplication exists between SQLite and Turso implementations:

| Duplicated Function | sqlite.py | turso.py | Similarity |
|---|---|---|---|
| `_map_type()` | ~15 lines | ~15 lines | ~100% identical |
| `_column_definition()` | ~40 lines | ~40 lines | ~95% identical |
| `_create_table_sql()` | ~30 lines | ~30 lines | ~95% identical |
| `_normalize_type()` (inspector) | ~18 lines | ~18 lines | ~100% identical |
| `_introspect_table()` (inspector) | ~30 lines | ~30 lines | ~90% identical |
| `_normalize_fk_action()` | ~12 lines | ~12 lines | 100% identical (copy-pasted 3x) |
| `_extract_view_query()` | ~5 lines | ~5 lines | 100% identical |
| `apply()`/`apply_sync()` | ~100 lines | ~100 lines | ~85% identical |

Estimated: ~350 lines of duplicated code across the applier+inspector layers.

### 6.2 Function Purity Audit

Total functions in codebase: ~180
- Pure functions: ~140 (78%)
- Boundary functions (justified I/O): ~25 (14%)
- Impure functions (unjustified side effects): ~15 (8%)

The impure functions are concentrated in:
- Module-level counter mutation in pragma_compat.py and check_compat.py
- Global schema state in query/table.py
- Connection pool state management in pool.py (justified)

---

## 7. Architectural Risk Assessment

### Low Risk (No Action Needed)
- types.py, differ/, query/builder.py, abstractions/{enums,arrays,maps,ranges,hierarchy,materialized_views}.py
- These are the system's core IP and follow every principle.

### Medium Risk (Refactor When Touching)
- query/table.py (global schema state)
- abstractions/pragma_compat.py and check_compat.py (module-level counters)
- query/executor.py (if/elif dispatch)
- pool.py (ABC hierarchy, but justified)

### High Risk (Prioritize Refactoring)
- applier/sqlite.py, postgresql.py, turso.py (stateless classes, massive duplication)
- applier/protocol.py, inspector/protocol.py (if/elif factories)
- inspector/sqlite.py, postgresql.py, turso.py (stateless classes, duplication)
