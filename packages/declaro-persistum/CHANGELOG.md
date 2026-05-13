# Changelog

All notable changes to `declaro-persistum` are recorded here.

## 0.1.6 — 2026-05-13

### Bugfixes
- **`update_many(..., increment=...)` crashed on Turso / MVCC pools** with
  `TypeError: object of type 'int' has no len()`. Root cause was in the
  executor: every write op on a pool with ``acquire_write`` was routed
  through ``_execute_update`` (cursor rowcount path), regardless of
  whether the SQL had a ``RETURNING`` clause. Reported via downstream
  bug report; thank you.
- **`update_one` / `create` / `delete` silently returned `int` instead of
  the documented `dict | None`** on Turso / MVCC pools, for the same
  reason. The bug surfaced loudly only in `update_many`'s `len()` call,
  but the others were silently corrupting return types — any consumer
  that dereferenced the result on Turso would have hit
  `TypeError: 'int' object is not subscriptable`.
- **Fix:** the executor now consults `has_returning_clause(sql)` for
  write ops on `acquire_write` pools. SQL with `RETURNING` is routed
  through the fetch path (rows) on the write connection; SQL without
  `RETURNING` keeps the count path (int rowcount). One dispatch
  decision, two correct behaviors. Same fix resolves all four reported
  symptoms.

### Honest Code refactors (no behavior change for honest callers)
- `_compute_schema_hash(schema_path, version)` — version is now passed as
  a parameter rather than read from a module-level constant inside the
  function. Tests no longer monkeypatch `declaro_persistum.__version__`
  to verify version-mixing; they call the pure function with explicit
  version arguments. (Honest Code Rule 11: Configuration as Parameters.)
- `_dialect_needs_orphan_recovery(dialect) -> bool` — the dispatch
  decision that gates the SQLite-specific orphaned-tmp-table recovery
  scan is now a pure helper. Tests assert it directly instead of
  monkeypatching `_recover_orphaned_tmp_tables` and using a fake pool
  with a sentinel exception to short-circuit `apply_migrations_async`.
- `compose_update_values(data, increment)` — moved from method on
  `PrismaQueryBuilder` to module-level pure function. The method form
  read nothing from `self` and was masquerading as instance-tied.
  (Honest Code Rule 3: Pure Functions Over Methods.)
- `has_returning_clause(sql)` — new pure helper in
  `declaro_persistum.instrumentation`, used by the executor to route
  write ops. Tested with whole-word matching to prevent false positives
  on column / table names containing the substring `returning`.

### Test cleanups
- `test_dirty_when_hash_matches_but_no_user_tables` previously declared
  a Pydantic model with `class Meta: table_name = 'users'`, which
  declaro's loader does not recognize. The loader returned an empty
  schema, the empty-schema branch of `_schema_is_clean` fired, and the
  test silently asserted on the wrong code path. Now uses
  `__tablename__` (the actual convention) so the test exercises what
  its docstring claims.
- `test_migrations_dialect_dispatch.py` and `test_schema_hash_version.py`
  rewritten as pure-function assertions against the new helpers
  (`_dialect_needs_orphan_recovery`, `_compute_schema_hash(schema, version)`)
  — no monkeypatching, no fake pools, no sentinel exceptions.

## 0.1.5 — 2026-05-13

### Features
- **Atomic `increment={"col": delta}`** on Prisma-style `update_one` /
  `TableProxy.update_one`. Emits `SET col = col + :inc_col` so the read and
  the write happen inside a single statement — no application-side RMW
  round trip, no race window between concurrent writers, no need to fetch
  the old value first. Negative deltas are supported (the signed value
  binds to the parameter; SQL stays `col + :param`). `data=` and
  `increment=` compose in the same UPDATE.
- **`update_many(where=, data=, increment=) -> int`** on Prisma-style API
  and `TableProxy`. Applies a uniform update to every matching row in one
  statement and returns the count of rows updated. Replaces the
  `1 + N`-round-trip pattern (one batched read + N per-row updates) with
  a single UPDATE for the uniform-delta case. Counter maintenance against
  large `IN` lists is the motivating use case (e.g. tag card-counts).
- **`increment(delta)` factory** exported at the top level
  (`from declaro_persistum import increment`). Pass as a value in the
  native `UpdateQuery` API for fluent atomic increments:
  `items.update(card_count=increment(1)).where(...).execute()`. Same
  semantics whether you use the native, Prisma, or `TableProxy` surface.

### Tests
- `tests/unit/test_increment_and_update_many.py` covers SQL emission,
  composition with `data=`, negative deltas, integration against real
  SQLite for atomicity, `update_many` row-count semantics, error cases
  (missing data/increment, column-in-both, unknown column), and hook
  integration (post-hook row count flows through to the returned count).

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
