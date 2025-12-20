# Declaro Stack - Design Conversation (2025-12-19)

## Summary

This conversation established the vision and architecture for the Declaro functional Python stack.

---

## Key Decisions

### 1. Monorepo Structure
- Single repo `declaro` containing all packages
- Packages: `declaro-persistum`, `declaro-ximinez`, `declaro-api`
- Reserved `declaro` on PyPI (v0.0.1 published)
- GitHub repo created: `github.com/adamzwasserman/declaro` (private)

### 2. Philosophy: Pure Functional Python
- **No classes** - TypedDicts instead of Pydantic/SQLAlchemy models
- **Pure functions** - No side effects, no mutation
- **Explicit types** - Ximinez enforces, TOML defines
- **Single source of truth** - TOML schema drives everything

### 3. Architecture

```
TOML Schema (source of truth)
    ↓ generates
TypedDict definitions
    ↓ used in
Python code with Ximinez types: blocks
    ↓ validated by
Ximinez at pre-commit (variables, functions, AND model usage)
```

---

## Why Pure Functions Beat Classes

### Encapsulation
- Classes: "private" members are convention only (`user._email = "hacked"` works)
- Closures: mathematically perfect encapsulation, truly inaccessible

### Polymorphism
- Classes: require interface inheritance, ceremony
- Functions: structural compatibility, duck typing - if types match, it works

### Inheritance/Reuse
- Classes: rigid tree hierarchy, fragile base class problem
- Functions: flexible composition, chain functions as needed

### Testing
- Classes: mocks, fixtures, setup/teardown, state management
- Pure functions: `assert f(input) == expected_output` - done

### State Corruption
- "A class is a petri dish for state corruption"
- "A dict is just data"
- Every method is a potential mutation site, every reference a liability

---

## Materialized View Emulation (Completed)

Implemented table-based emulation for SQLite/Turso materialized views:

### Files Created
- `src/declaro_persistum/abstractions/materialized_views.py`
- `tests/bdd/features/schema/materialized_views.feature`
- `tests/unit/test_materialized_views.py`

### Features
- Backing table created from `CREATE TABLE AS query`
- Metadata table `_dp_materialized_views` tracks emulated views
- Three refresh strategies: `manual`, `trigger`, `hybrid`
- Auto-refresh triggers for trigger/hybrid strategies
- Inspector detects emulated matviews from metadata

### New View Fields (types.py)
```python
class View(TypedDict, total=False):
    name: str
    query: str
    materialized: bool
    refresh: Literal["on_demand", "on_commit", "manual", "trigger", "hybrid"]
    depends_on: list[str]
    trigger_sources: list[str]  # NEW - for trigger-based refresh
```

---

## Ximinez Integration Vision

Ximinez = Python type enforcer with Spanish Inquisition themed errors

### Two Typing Styles (Mutually Exclusive per Function)

**Inline:**
```python
def confess(x: int, y: str) -> float:
    sins: int = 0
    penance: float = 0.0
    return penance
```

**Block declaration:**
```python
def confess(x: int, y: str) -> float:
    types:
        sins: int = 0
        penance: float
        fear: bool = True

    # ... code ...
    return penance
```

### Integration with Declaro
- TOML defines models (structure, relationships, validation)
- Ximinez enforces types in Python code (variables, functions)
- At pre-commit, Ximinez ALSO validates model usage matches TOML

### Model Validation Examples
```
NOBODY expects a model violation!

Our TWO chief violations are:
- app.py:42:5: 'User' has no field 'username' (did you mean 'name'?)
- app.py:47:9: 'Order' relationship not declared on 'User'
```

---

## FastAPI Integration Plan

### The Challenge
FastAPI is married to Pydantic. Need to provide functional alternative.

### Strategy: Plugin First (POC for Sebastian)
1. Create `declaro-fastapi` package
2. Works WITH FastAPI, not fork
3. Provides `Body[Model]`, `Response[Model]` that validate against TOML
4. If gains traction, propose upstream changes

### Plugin API Sketch
```python
from fastapi import FastAPI
from declaro_fastapi import init_models, Body
from models import User  # Generated TypedDict

app = FastAPI()
init_models(app, "schema/models/*.toml")

@app.post("/users", response_model=User)
async def create_user(user: Body[User]) -> User:
    return await insert("users", user)
```

### Key Features Plugin Must Prove
- Full OpenAPI support (TOML → JSON Schema)
- Performance at least as good as Pydantic
- Works alongside Pydantic (gradual adoption)
- Clear benefits visible

---

## Replacing Pydantic with TOML + Pure Functions

### What Pydantic Does → Functional Alternative

| Pydantic | Declaro |
|----------|---------|
| Schema definition (class) | TOML schema |
| Validation (`__init__` magic) | `validate(schema, data) → errors` |
| Coercion (field descriptors) | `coerce(schema, data) → data` |
| Serialization (`.model_dump()`) | Already just a dict |
| Relationships (nested classes) | TOML relationships |

### Single Source of Truth
```toml
# schema/models/user.toml
[user]
table = "users"

[user.fields]
id = { type = "uuid" }
email = { type = "str", validate = ["email"] }
name = { type = "str", nullable = true }

[user.relationships]
orders = { type = "has_many", target = "order", foreign_key = "user_id" }
```

Generates:
- TypedDict for Python typing
- Validation functions
- Database schema
- OpenAPI/JSON Schema
- Query builders with relationship awareness

---

## Lazy Loading Analysis

**Rejected** - breaks pure functional design.

### Why Lazy Loading is Problematic
1. **Hidden I/O**: `user.orders` looks like data access, secretly runs SQL
2. **Non-deterministic**: Same code, different results based on timing/caching
3. **Requires mutable state**: Objects track "have I loaded this?"
4. **Connection dependency**: Objects "attached" to sessions, break when detached

### Alternative: Explicit Eager Loading
```python
user = await query.select("users").include("orders").where(id=1).one()
# Returns pure data with orders already loaded, single query
```

---

## Roadmap

### Phase 1: Query Power
- Explicit JOIN builder
- Subquery support
- CTEs (WITH clauses)
- Window functions

### Phase 2: Relationship DSL
- Declare relationships in TOML
- Generate join queries automatically
- Include/select syntax (Prisma-style)
- NO lazy loading - explicit only

### Phase 3: Performance
- Query result caching (optional wrapper)
- Prepared statement caching
- Connection pooling improvements
- Batch operations

---

## Key Quotes

> "A class is a petri dish for state corruption. A dict is just data."

> "Pure functions don't simulate encapsulation and polymorphism. They *are* encapsulation and polymorphism, without the state corruption."

> "Objects are state wrapped in methods pretending to be data."

> "If you need a debugger, your code is too clever."

> "Testability is not a feature. It's a consequence of purity."

---

## Files Modified/Created This Session

### declaro_persistum (committed)
- `src/declaro_persistum/abstractions/materialized_views.py` - NEW
- `src/declaro_persistum/types.py` - Added trigger_sources, extended refresh
- `src/declaro_persistum/loader.py` - Validation for new strategies
- `src/declaro_persistum/applier/sqlite.py` - Matview emulation
- `src/declaro_persistum/applier/turso.py` - Matview emulation
- `src/declaro_persistum/inspector/sqlite.py` - Detect emulated matviews
- `src/declaro_persistum/abstractions/__init__.py` - Exports
- `tests/unit/test_materialized_views.py` - NEW (29 tests)
- `tests/bdd/features/schema/materialized_views.feature` - NEW
- `docs/usage.md` - Updated with matview docs

### declaro monorepo (created)
- `README.md`
- `MANIFESTO.md`
- `LICENSE`
- `pyproject.toml`
- `src/declaro/__init__.py`
- `.gitignore`
- `packages/` directory structure

---

## Next Steps (TODO)

1. ~~Revoke exposed PyPI token~~ (user should do this)
2. Commit declaro monorepo initial structure
3. Move declaro_persistum into `packages/declaro-persistum/`
4. Rename package from `declaro_persistum` to `declaro-persistum`
5. Start ximinez implementation in `packages/declaro-ximinez/`
6. Create `declaro-api` FastAPI plugin skeleton

---

## PyPI Package

- **Name**: `declaro`
- **Version**: 0.0.1
- **URL**: https://pypi.org/project/declaro/0.0.1/
- **Status**: Placeholder published, name reserved

## GitHub Repo

- **URL**: https://github.com/adamzwasserman/declaro
- **Visibility**: Private
- **Status**: Created, initial commit pending
