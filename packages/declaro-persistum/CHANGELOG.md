# Changelog

All notable changes to `declaro-persistum` are recorded here.

## 0.1.4 — 2026-04-28

### Bugfixes
- **Skip-if-clean cache hid loader/applier fixes from upgrades.** The
  schema-hash optimization stored a hash representing "the result of
  running this version of declaro against this file." After a buggy
  version stamped a "clean" hash, upgrading to a fixed version did
  nothing on next startup — the hash still matched the unchanged source
  file, so the runner skipped re-introspection and the corrupted schema
  silently persisted until the user edited their model file or passed
  `force=True`. The 0.1.3 PEP-563 fix was visible to consumers only after
  manual cache invalidation.
- **Fix:** `_compute_schema_hash` now mixes `declaro_persistum.__version__`
  into the hash input (with a NUL delimiter so file content cannot collide
  with the version string). Any version bump invalidates the cache,
  triggering exactly one re-introspection pass on first startup after an
  upgrade. Cost is milliseconds for typical schemas; the alternative is
  silent persistence of bugs across upgrades.

### Operational note
- After upgrading from 0.1.3 (or earlier) to 0.1.4, your app will perform
  one introspection pass on first startup even if your schema file is
  unchanged. This is intentional — it ensures any fixes shipped in 0.1.4
  (or future versions) take effect against your existing database. No
  action required.

### Internal
- `__version__` moved to the top of `declaro_persistum/__init__.py`
  (above submodule imports) so submodules can read it without circular
  imports.
- Regression tests added in `tests/unit/test_schema_hash_version.py`.

## 0.1.3 — 2026-04-28

### Bugfixes
- **`bool` columns silently became `text`; `T | None` columns silently became
  `NOT NULL`** — for any model file that used `from __future__ import
  annotations` (PEP 563) or any string forward reference. `pydantic_loader`
  read `cls.__annotations__` directly, which under PEP 563 returns *strings*
  ("bool", "datetime | None") rather than types. Strings missed every
  type-keyed lookup in `python_type_to_sql` (falling through to the `text`
  default) and `is_optional_type` couldn't introspect them (returning False
  for every union). The result: silent schema corruption — wrong column
  types and NOT NULL where the user wrote `T | None`. The loader now uses
  `typing.get_type_hints(model_cls)`, which resolves string annotations
  against the model's module globals regardless of PEP 563. Falls back to
  `__annotations__` only if `get_type_hints` raises (unresolvable forward
  ref). Reported via downstream bug report; thank you.
- Regression tests added in `tests/unit/test_pydantic_loader_pep563.py`
  cover `bool`, `T | None`, and a byte-identity check between PEP-563 and
  non-PEP-563 model files.

## 0.1.2 — 2026-04-28

### Bugfixes
- **Crash on Postgres at lifespan startup.** `apply_migrations_async` called
  `_recover_orphaned_tmp_tables(pool)` as a pre-flight unconditionally, which
  queries `sqlite_master`. On Postgres this raised
  `asyncpg.exceptions.UndefinedTableError: relation "sqlite_master" does not
  exist` before any actual migration logic ran, breaking every Postgres-backed
  app at startup. The recovery scan now dispatches by dialect and only runs
  for `sqlite` and `turso`. Postgres reconstruction does not produce these
  temp tables, so the scan is meaningless there. Reported via downstream
  bug report; thank you.
- Regression tests added in `tests/unit/test_migrations_dialect_dispatch.py`
  cover all three supported dialects.

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
