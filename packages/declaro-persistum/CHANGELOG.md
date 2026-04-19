# Changelog

All notable changes to `declaro-persistum` are recorded here.

## 0.1.1 — 2026-04-19

### Documentation
- README: new "Query Hooks (pre / post)" section with the function-passing
  design explained and an end-to-end RLS / audit example.
- `docs/hooks.md`: expanded "Design: hooks are passed in, not registered"
  section explaining why hook functions are passed as arguments rather than
  registered via decorators — no module-level registry, no import-time
  side effects, every hook traceable to a call site.

### Internal
- `query/prisma_style.py::PrismaQueryBuilder._where_to_conditions` returns
  `list[Condition | ConditionGroup]` rather than `list[Condition]` with a
  `type: ignore[arg-type]` override. Same runtime behavior, honest types.

## 0.1.0 — 2026-04-19

Initial public release on PyPI.

### Core
- Schema-first migration toolkit: Pydantic models → `types.py` TypedDict
  schemas → protocol-based inspector/differ/applier per dialect.
- Unified `ConnectionPool` API across PostgreSQL (asyncpg), SQLite
  (aiosqlite), and Turso (pyturso, with optional cloud sync).
- Fluent query builder with native, Django-style, Prisma-style, and
  SQLAlchemy-compat surfaces — all schema-validated at build time.
- Enum abstraction: `Literal[...]` types auto-generate FK-constrained
  lookup tables.

### Query hooks
- `table_factory(schema, pool, *, pre=None, post=None)` — binds pre-hook
  and post-hook functions to every query built from the returned factory.
- `PreHook = (query) -> query` and `PostHook = (rows, QueryMeta) -> rows`.
- Pre-hooks can structurally rewrite queries (e.g. DELETE → UPDATE for
  soft delete) by returning a different query type.
- `.execute(pre=..., post=..., without_hooks=...)` override or bypass at
  call time.
