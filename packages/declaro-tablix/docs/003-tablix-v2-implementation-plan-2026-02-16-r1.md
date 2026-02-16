# Tablix v2 Implementation Plan

## Document ID: 003
## Date: 2026-02-16
## Revision: 1

---

## 1. Executive Summary

Tablix v2 transforms the current codebase from a monolithic table rendering system with scattered infrastructure concerns into a focused, high-performance two-tier caching architecture with pure functional patterns. The migration follows a removal-first strategy: deprecated code is deleted before replacement code is written, ensuring no dead code accumulates and every module serves its purpose.

Four work streams execute in sequence:

1. **Remove Python Number Formatting** -- Delete the entire `formatting/` package and financial integration code, relying entirely on genX fmtx.js for client-side formatting
2. **Remove SQLAlchemy Direct Usage** -- Replace all SQLAlchemy ORM and raw SQL with declaro-persistum ConnectionPool and fluent query API
3. **Implement Two-Tier Cache Architecture** -- Build the cache/keys, cache/tier1, cache/tier2, merge engine, and invalidation modules per the v2 cache architecture specification
4. **CSS Styling Re-engineering** -- Evolve the current CSS variable system into an introspectable design token system

---

## 2. Current State Analysis

### 2.1 Codebase Inventory

The current tablix package contains the following module groups:

| Module Group | Files | Status | v2 Disposition |
|-------------|-------|--------|----------------|
| `domain/models.py` | 1 | Stable, well-designed | KEEP -- core Pydantic models |
| `domain/filter_layout.py` | 1 | Stable, well-designed | KEEP -- 13 filter control types |
| `domain/financial_columns.py` | 1 | Uses class inheritance | REMOVE -- server-side formatting |
| `formatting/` | 6 files | Server-side formatters | REMOVE ENTIRELY |
| `financial_integration.py` | 1 | Deprecated | REMOVE |
| `api_integration.py` | 1 | Deprecated | REMOVE |
| `templates/__init__.py` | 1 | fmtx.js helpers present | KEEP -- already v2-aligned |
| `templates/components/` | 6+ files | fx-* attributes present | KEEP -- already v2-aligned |
| `styling/` | 3 files | CSS variables, viewport scaling | EVOLVE -- design token system |
| `caching/` | 3 files | Synchronous Redis, classes, globals | REMOVE, REPLACE with two-tier |
| `repositories/cache_repository.py` | 1 | Synchronous Redis, globals | REMOVE, REPLACE |
| `repositories/table_repository.py` | 1 | Uses persistum Prisma-style | KEEP -- already v2-aligned |
| `repositories/table_data_repository.py` | 1 | SQLAlchemy raw SQL | REPLACE with persistum |
| `repositories/table_config_repository.py` | 1 | SQLAlchemy raw SQL | REPLACE with persistum |
| `repositories/customization_repository.py` | 1 | SQLAlchemy raw SQL | REPLACE with persistum |
| `repositories/preference_repository.py` | 1 | SQLAlchemy raw SQL | REPLACE with persistum |
| `customization/models.py` | 1 | SQLAlchemy ORM classes | REPLACE with persistum @table |
| `customization/persistence.py` | 1 | SQLAlchemy Session | REPLACE with persistum |
| `customization/validators.py` | 1 | Pydantic validators | KEEP -- validation logic sound |
| `routes/table_routes.py` | 1 | References formatting_strategy | REWRITE for v2 |
| `routes/models.py` | 1 | References FormattingStrategy | REWRITE for v2 |
| `plugins/` | 13 files | Global registries, classes | EVALUATE -- may simplify |
| `security/` | 6 files | Formula security, input validation | KEEP -- review for async |
| `monitoring/` | 4 files | Health checks, metrics | KEEP -- update for two-tier |
| `performance/` | 8 files | Benchmarks, load testing | KEEP -- update for two-tier |
| `services/formula_engine.py` | 1 | Excel-like formulas | KEEP -- isolated |
| `models.py` | 1 | @table persistum models | KEEP -- expand for v2 |

### 2.2 Architectural Violations in Current Code

The following violations of the declared architecture must be corrected in v2:

**Class Usage Violations (business logic in classes):**
- `caching/cache_invalidation.py`: `CacheInvalidationManager` class with mutable state
- `caching/cache_middleware.py`: `TableCacheMiddleware` class, `CacheConfig` class
- `customization/models.py`: 4 SQLAlchemy ORM classes (`ColumnCustomization`, `UserTablePreferences`, `CustomizationHistory`, `CustomizationTemplate`)
- `plugins/protocols.py`: `PluginRegistry`, `PluginHookHandler`, `PluginManifest` classes with mutable state
- `domain/financial_columns.py`: `FinancialColumnDefinition` uses class inheritance

**Global Mutable State Violations:**
- `caching/cache_service.py`: `_cache_metrics` global mutable dict
- `caching/cache_invalidation.py`: `_invalidation_manager` global instance
- `plugins/protocols.py`: `_global_plugin_registry` global mutable instance
- `plugins/plugin_manager.py`: `_plugin_system_config` global mutable dict
- `repositories/cache_repository.py`: `_redis_client`, `_connection_pool` globals

**Synchronous I/O Violations:**
- `repositories/cache_repository.py`: Synchronous Redis operations
- `repositories/table_data_repository.py`: Synchronous SQLAlchemy
- `repositories/table_config_repository.py`: Synchronous SQLAlchemy
- `customization/persistence.py`: Synchronous SQLAlchemy Session

**SQLAlchemy Direct Usage (to be replaced by persistum):**
- `customization/models.py`: SQLAlchemy Column, Base, etc.
- `customization/persistence.py`: SQLAlchemy Session, query, filter
- `repositories/table_data_repository.py`: SQLAlchemy text(), Session
- `repositories/table_config_repository.py`: SQLAlchemy text(), Session
- `pyproject.toml`: `sqlalchemy>=2.0.0` dependency

### 2.3 What is Already v2-Aligned

These modules require no changes or minimal changes:

- `domain/models.py`: All Pydantic models with proper validation
- `domain/filter_layout.py`: FilterControlType, FilterPosition, FilterControlConfig, FilterLayoutConfig
- `templates/__init__.py`: `get_fmtx_script_tag()`, `needs_fmtx()`, `render_table_ui()`
- `templates/components/table.html`: Already emits `fx-format`, `fx-currency`, `fx-decimals` attributes
- `styling/filter_variables.css`: CSS custom properties with viewport scaling
- `styling/filter_components.css`: Component styles using CSS variables
- `repositories/table_repository.py`: Already uses persistum Prisma-style API
- `models.py`: Already uses persistum @table decorator

---

## 3. Gap Analysis

### 3.1 Formatting Gap (Requirement: Remove Python Number Formatting)

**Current State:** 6 files in `formatting/` providing server-side number, currency, percentage, and date formatting via Python. Routes reference `FormattingStrategy`. Route models import from formatting.

**Target State:** All formatting handled by genX fmtx.js client-side via `fx-*` HTML attributes. No Python formatting code exists. Templates emit raw numeric values with `fx-format` attributes.

**Gap:** The `formatting/` package, `financial_integration.py`, `api_integration.py`, `domain/financial_columns.py`, and all references to `FormattingStrategy` / formatting functions in routes must be deleted. The template layer is already v2-aligned (emits fx-* attributes).

**Files to Remove:**
- `src/declaro_tablix/formatting/__init__.py`
- `src/declaro_tablix/formatting/cell_formatters.py`
- `src/declaro_tablix/formatting/financial_formatters.py`
- `src/declaro_tablix/formatting/formatter_registry.py`
- `src/declaro_tablix/formatting/formatting_strategy.py`
- `src/declaro_tablix/formatting/style_generators.py`
- `src/declaro_tablix/financial_integration.py`
- `src/declaro_tablix/api_integration.py`
- `src/declaro_tablix/domain/financial_columns.py`

**Files to Edit (remove formatting references):**
- `src/declaro_tablix/routes/table_routes.py`: Remove formatting_strategy imports, apply_formatting_strategy calls
- `src/declaro_tablix/routes/models.py`: Remove FormattingStrategy import, FormattingRequest/FormattingResponse models

### 3.2 Persistum Migration Gap (Requirement: All DB via persistum)

**Current State:** Some modules use persistum (table_repository.py, models.py). Most database access uses SQLAlchemy directly (raw SQL with `text()`, ORM sessions, `Session.query()`).

**Target State:** ALL database access uses declaro-persistum ConnectionPool + fluent query API. No SQLAlchemy imports in any tablix module. `sqlalchemy` removed from pyproject.toml dependencies.

**Gap:** All SQLAlchemy-based repositories and models must be rewritten to use persistum's `table()`, `select()`, `where()`, `execute()` API. The customization module's SQLAlchemy ORM models must be converted to persistum @table Pydantic models.

**Files Requiring Full Rewrite:**
- `src/declaro_tablix/repositories/table_data_repository.py`: SQLAlchemy -> persistum query API
- `src/declaro_tablix/repositories/table_config_repository.py`: SQLAlchemy -> persistum query API
- `src/declaro_tablix/repositories/customization_repository.py`: SQLAlchemy -> persistum query API
- `src/declaro_tablix/repositories/preference_repository.py`: SQLAlchemy -> persistum query API
- `src/declaro_tablix/customization/models.py`: SQLAlchemy ORM -> persistum @table
- `src/declaro_tablix/customization/persistence.py`: SQLAlchemy Session -> persistum ConnectionPool

### 3.3 Two-Tier Cache Gap (Requirement: v2 Cache Architecture)

**Current State:** Single-tier cache with synchronous Redis, global mutable state, class-based invalidation manager, Starlette middleware approach.

**Target State:** Two-tier cache per the v2 architecture specification: Tier 1 (shared data, long TTL), Tier 2 (per-user indexes/overlays, short TTL), merge engine in application code, deterministic key generation, async Redis, graceful degradation.

**Gap:** The entire caching subsystem must be replaced. The current `caching/` directory (3 files) and `repositories/cache_repository.py` are removed. New modules are created following the v2 specification:

**New Modules:**
- `src/declaro_tablix/cache/keys.py`: Deterministic cache key generation
- `src/declaro_tablix/cache/tier1.py`: Shared data manager (fund records, skeletons)
- `src/declaro_tablix/cache/tier2.py`: Per-user data manager (indexes, overlays)
- `src/declaro_tablix/cache/warmer.py`: Startup + scheduled warming
- `src/declaro_tablix/cache/invalidation.py`: Write-through invalidation hooks
- `src/declaro_tablix/cache/config.py`: TablixConfig (frozen dataclass)
- `src/declaro_tablix/merge/screener.py`: Index slice + record hydration
- `src/declaro_tablix/merge/views.py`: Skeleton + overlay tree walk

### 3.4 CSS Styling Gap (Requirement: Design Token System)

**Current State:** CSS custom properties in `filter_variables.css` with viewport scaling (50+ variables). Component styles in `filter_components.css` (~670 lines). `styling/__init__.py` provides `get_filter_css()` which concatenates both files. Covers filters only — no table, pagination, or header styling. No programmatic token input. No introspectable class name registry. Consumers override by redefining `:root` variables manually. No headless/unstyled mode. Dark mode uses duplicated `@media (prefers-color-scheme)` + `.dark` class blocks. Reduced-motion and high-contrast handled via `@media` queries (will be removed — genX handles a11y).

**Target State:**
- Design token system covering ALL components (table, filters, pagination, headers)
- **Hybrid B+C naming**: ~30-40 public semantic tokens (`--tablix-color-primary`, `--tablix-spacing-gap`) as the contract, with component-level opt-in overrides via `var(--tablix-search-radius, var(--tablix-radius))` fallback pattern
- **Single compiled source**: Consumer picks ONE input method — JSON/YAML file OR Python dict (not both). Compiler enforces single-source. CSS cascade always wins at runtime.
- **Three output modes**: `themed` (full visual), `minimal` (structural + spacing), `headless` (structural only)
- **`light-dark()` notation**: All color tokens use CSS `light-dark()` function. No `@media (prefers-color-scheme)` blocks, no `.dark` class duplication. `color-scheme: light dark` on `:root`.
- **No a11y @media blocks**: genX/fmtx.js handles reduced-motion and high-contrast. Strip those blocks entirely.
- **Auto-compile on startup**: Zero build step for development. CSS file generated on first import/app boot.
- **CSS delivery**: Both static file helpers (`get_css_path()`, `get_css_link_tag()`) and FastAPI static mount option.
- **Inline style overrides**: Dict of CSS selectors → properties passed at render time, complementing tokens for per-instance overrides. `style_mode` switch: `"inline"` (emits `<style>` block) or `"class"` (generates named override classes).
- **Introspection API**: `get_class_names()`, `get_token_contract()`, `get_component_map()` + generated JSON manifest.
- **Heavily commented output**: Generated CSS includes extensive comments documenting every token, its purpose, and how to override — optimized for AI coding assistants.

**Gap:**
1. **Scope expansion**: Current CSS covers filters only; must extend to table, pagination, headers
2. **Token input pipeline**: No mechanism exists to accept tokens as Python dict or JSON/YAML file — must build a compilation pipeline (tokens → CSS file)
3. **Naming convention**: Current tokens use `--filter-*` prefix; must migrate to `--tablix-*` with hybrid B+C pattern
4. **light-dark() conversion**: Current dark mode uses duplicated `@media` + `.dark` blocks; must convert all color tokens to `light-dark()` notation
5. **a11y cleanup**: Remove `@media (prefers-reduced-motion)` and `@media (prefers-contrast: high)` blocks — genX handles these
6. **Three modes**: No way to strip visual styles while keeping structural CSS — must separate structural, spacing, and visual layers in the component CSS
7. **Introspection API**: No programmatic surface exists — class names and tokens are only discoverable by reading CSS source
8. **Inline override mechanism**: No way to inject per-instance style overrides at render time
9. **Auto-compilation**: Current approach reads CSS files at runtime via `get_filter_css()` — target auto-compiles on startup and serves static .css

**Foundation to build on:** The existing CSS variable system is well-structured with clear categories (spacing, dimensions, typography, colors, borders, shadows, transitions, z-index). Current variable values become the default "themed" mode. The viewport-based `clamp()` scaling system is sound and carries forward.

---

## 4. Implementation Phases

### Phase 0: Pre-Flight (Safety Net)

Before any code changes, establish the safety net:

1. Run all existing tests and record baseline
2. Verify test coverage numbers
3. Ensure demo app runs correctly
4. Document all import chains that will break during removal

### Phase 1: REMOVAL -- Delete Deprecated Code

**Principle:** Remove code FIRST, then build. This prevents dead code accumulation and ensures every module serves its purpose.

#### Task 1.1: Remove Python Formatting Package
**Priority:** P0 -- Blocks all subsequent work
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours

Steps:
1. Delete `src/declaro_tablix/formatting/` (entire directory, 6 files)
2. Delete `src/declaro_tablix/financial_integration.py`
3. Delete `src/declaro_tablix/api_integration.py`
4. Delete `src/declaro_tablix/domain/financial_columns.py`
5. Remove all imports of formatting modules from other files
6. Remove `FormattingStrategy` import from `routes/models.py`
7. Remove formatting-related imports from `routes/table_routes.py`
8. Remove `FormattingRequest` and `FormattingResponse` from route models
9. Remove the `/tables/format` route endpoint
10. Clean up `__init__.py` files that reference removed modules
11. Verify template layer still works (fx-* attributes are template-level, not Python)
12. Run remaining tests (expect some failures from removed imports)

**Verification:** `uv run python -c "import declaro_tablix"` succeeds. Template rendering still produces fx-* attributes.

#### Task 1.2: Remove Old Caching Infrastructure
**Priority:** P0 -- Blocks cache v2 work
**Agent:** Build Agent
**Estimated Effort:** 1-2 hours

Steps:
1. Delete `src/declaro_tablix/caching/cache_service.py`
2. Delete `src/declaro_tablix/caching/cache_invalidation.py`
3. Delete `src/declaro_tablix/caching/cache_middleware.py`
4. Delete `src/declaro_tablix/caching/__init__.py`
5. Delete `src/declaro_tablix/repositories/cache_repository.py`
6. Remove cache-related imports from route handlers
7. Remove cache-related endpoints from routes (`/tables/cache`)
8. Remove `CacheRequest`/`CacheResponse` from route models
9. Clean up any references to removed cache modules

**Verification:** Package imports without cache modules. Routes work without cache layer (direct DB queries).

#### Task 1.3: Identify and Tag SQLAlchemy Usage
**Priority:** P0 -- Informational, supports Phase 2
**Agent:** Architect Agent
**Estimated Effort:** 1 hour

Steps:
1. Grep all `from sqlalchemy` and `import sqlalchemy` references
2. Grep all `from declaro_persistum.compat` references (these use SQLAlchemy under the hood)
3. Document each file and function that uses SQLAlchemy directly
4. Create dependency graph showing which modules depend on SQLAlchemy
5. Determine migration order (leaf modules first, then dependents)

### Phase 2: MIGRATE -- SQLAlchemy to Persistum

#### Task 2.1: Convert Customization Models to Persistum @table
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 3-4 hours
**Dependency:** Task 1.1, 1.2 complete

Steps:
1. Rewrite `customization/models.py` using persistum `@table` decorator and Pydantic models
2. Convert `ColumnCustomization` SQLAlchemy model -> `ColumnCustomizationModel` Pydantic @table
3. Convert `UserTablePreferences` -> `UserTablePreferencesModel` Pydantic @table
4. Convert `CustomizationHistory` -> `CustomizationHistoryModel` Pydantic @table
5. Convert `CustomizationTemplate` -> `CustomizationTemplateModel` Pydantic @table
6. Remove SQLAlchemy imports entirely from the file
7. Remove `to_dict()` methods (Pydantic models have `.model_dump()`)
8. Remove `__repr__` methods (Pydantic provides this)

**Verification:** Models can be imported. `load_schema_from_models()` produces correct schema.

#### Task 2.2: Convert table_data_repository to Persistum
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 3-4 hours
**Dependency:** Task 2.1

Steps:
1. Rewrite `repositories/table_data_repository.py` using persistum query API
2. Replace `Session` parameter with `ConnectionPool` / connection parameter
3. Replace `text()` SQL with persistum `table().select().where().execute()`
4. Make all functions `async` (persistum ConnectionPool is async)
5. Replace `information_schema` queries with persistum inspector
6. Remove all SQLAlchemy imports
7. Remove `get_db()` dependency injection pattern (use ConnectionPool instead)
8. Write pure functions with explicit ConnectionPool parameter

**Verification:** `get_table_data()` returns correct data using persistum. Async functions work with `await`.

#### Task 2.3: Convert table_config_repository to Persistum
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Task 2.1

Steps:
1. Rewrite `repositories/table_config_repository.py` using persistum query API
2. Replace all `text()` raw SQL with persistum `table().select()`, `table().insert()`, `table().update()`, `table().delete()`
3. Make all functions `async`
4. Replace `Session` with `ConnectionPool` parameter
5. Remove SQLAlchemy imports

**Verification:** CRUD operations on table_configurations work via persistum.

#### Task 2.4: Convert customization/persistence.py to Persistum
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 3-4 hours
**Dependency:** Task 2.1

Steps:
1. Rewrite `customization/persistence.py` using persistum query API
2. Replace `Session.query(ColumnCustomization).filter()` with persistum `table().select().where()`
3. Make all functions `async`
4. Replace `SessionLocal()` with `ConnectionPool` parameter
5. Remove the `should_close` pattern (persistum manages connections)
6. Remove SQLAlchemy imports

**Verification:** Customization CRUD works via persistum.

#### Task 2.5: Convert Remaining Repositories
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Task 2.1

Steps:
1. Convert `repositories/customization_repository.py` to persistum
2. Convert `repositories/preference_repository.py` to persistum
3. Ensure all repository functions are `async`
4. Ensure all take explicit `ConnectionPool` or connection parameter

**Verification:** All repository functions work via persistum.

#### Task 2.6: Remove SQLAlchemy Dependency
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 1 hour
**Dependency:** Tasks 2.1-2.5 all complete

Steps:
1. Final grep for any remaining `sqlalchemy` imports (should be zero)
2. Final grep for any remaining `declaro_persistum.compat` imports (should be zero or intentional)
3. Remove `sqlalchemy>=2.0.0` from `pyproject.toml` dependencies
4. Run `uv sync` to update lock file
5. Run full test suite to verify nothing breaks

**Verification:** `uv run python -c "import sqlalchemy"` fails (not installed). All tests pass.

### Phase 3: BUILD -- Two-Tier Cache Architecture

#### Task 3.1: Create Cache Key Generator
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 2 hours
**Dependency:** Phase 2 complete

Create `src/declaro_tablix/cache/keys.py`:
- Pure functions for deterministic cache key generation
- `fund_key(symbol: str) -> str` returns `"fund:{symbol}"`
- `skeleton_key(view_id: str) -> str` returns `"skeleton:{view_id}"`
- `user_index_key(user_id: str, query_hash: str) -> str` returns `"idx:{user_id}:{query_hash}"`
- `user_overlay_key(user_id: str, view_id: str) -> str` returns `"overlay:{user_id}:{view_id}"`
- `compute_query_hash(filters: FrozenSet[str], sort_column: str, sort_direction: str) -> str` using md5
- All functions are pure, no side effects, no state
- Comprehensive type hints

**Verification:** Unit tests confirm deterministic key generation. Same inputs always produce same keys.

#### Task 3.2: Create Cache Configuration
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 1 hour
**Dependency:** Task 3.1

Create `src/declaro_tablix/cache/config.py`:
- Frozen dataclass or frozen Pydantic model for `TablixCacheConfig`
- All TTL values, Redis URLs, compression settings
- Connection pool configuration
- Feature flags (warm_tier1_on_startup, compress_indexes)
- Factory function to create from environment variables

**Verification:** Config loads from environment. Immutable (frozen).

#### Task 3.3: Implement Tier 1 Shared Data Manager
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 4-5 hours
**Dependency:** Tasks 3.1, 3.2

Create `src/declaro_tablix/cache/tier1.py`:
- `warm_fund_cache(redis, pool, config) -> int` -- loads fund records into Redis
- `get_fund_record(redis, symbol) -> Optional[dict]` -- single fund lookup
- `get_fund_records(redis, symbols: list[str]) -> list[dict]` -- MGET for page hydration
- `warm_skeleton(redis, pool, view_id, config) -> dict` -- builds and caches skeleton
- `get_skeleton(redis, view_id) -> Optional[dict]` -- skeleton lookup
- `invalidate_funds(redis) -> int` -- delete all fund:* keys
- `invalidate_skeleton(redis, view_id) -> bool` -- delete specific skeleton
- All functions async, pure (no global state)
- Uses orjson for serialization
- Uses persistum ConnectionPool for DB access on cache miss

**Verification:** Fund cache loads 23K records. Skeleton builds correctly. MGET returns correct records.

#### Task 3.4: Implement Tier 2 Per-User Data Manager
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 4-5 hours
**Dependency:** Tasks 3.1, 3.2

Create `src/declaro_tablix/cache/tier2.py`:
- `get_or_create_index(redis, pool, user_id, query) -> list[str]` -- sorted symbol index
- `get_or_create_overlay(redis, pool, user_id, view_id) -> dict` -- user response overlay
- `invalidate_user_index(redis, user_id, query_hash) -> bool` -- delete specific index
- `invalidate_user_overlay(redis, user_id, view_id) -> bool` -- delete specific overlay
- `invalidate_all_user_data(redis, user_id) -> int` -- delete all Tier 2 keys for user
- All functions async, pure (no global state)
- Uses persistum query API for DB access on cache miss
- Supports optional LZ4 compression for large indexes

**Verification:** Index created on miss, served from cache on hit. Overlay created on miss, served from cache on hit. TTL expiry works correctly.

#### Task 3.5: Implement Merge Engine -- Screener
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 3-4 hours
**Dependency:** Tasks 3.3, 3.4

Create `src/declaro_tablix/merge/screener.py`:
- `serve_screener_page(redis, user_id, query, symbols) -> ScreenerResult`
- Paginates cached index by slicing
- Hydrates page from Tier 1 via MGET
- Pure function (no DB access, no cache writes)
- Handles missing fund records gracefully (MGET returns nil)
- Returns typed result with rows, total, page, limit

**Verification:** Pagination produces correct slices. Fund records hydrated correctly. Missing records handled.

#### Task 3.6: Implement Merge Engine -- Views
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 3-4 hours
**Dependency:** Tasks 3.3, 3.4

Create `src/declaro_tablix/merge/views.py`:
- `merge_view(skeleton, overlay) -> dict` -- attaches user responses to skeleton
- Pure function (no side effects, no DB access)
- Deep copies skeleton (never mutates original)
- Walks nested tree structure attaching overlay at leaf nodes
- Returns new merged tree

**Verification:** Merge produces correct nested document. Skeleton not mutated. Missing responses result in None values.

#### Task 3.7: Implement Cache Warmer
**Priority:** P2
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Tasks 3.3, 3.4

Create `src/declaro_tablix/cache/warmer.py`:
- `warm_tier1(redis, pool, config) -> WarmResult` -- startup warming
- `warm_tier2_for_users(redis, pool, user_ids, config) -> WarmResult` -- optional pre-warm
- Runs as background task (does not block server startup)
- Reports warming progress and any errors
- Idempotent (safe to run multiple times)

**Verification:** Tier 1 warms successfully at startup. Background task does not block.

#### Task 3.8: Implement Invalidation Handler
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Tasks 3.3, 3.4

Create `src/declaro_tablix/cache/invalidation.py`:
- `invalidate_on_user_write(redis, user_id, view_id) -> bool` -- Tier 2 only
- `invalidate_on_data_refresh(redis) -> int` -- Tier 1 funds
- `invalidate_on_regulatory_change(redis, view_id) -> bool` -- Tier 1 skeleton
- All functions are fire-and-forget safe
- Tier 1 and Tier 2 invalidation are completely independent
- No classes, no global state

**Verification:** User write invalidates only Tier 2 key. Data refresh invalidates only Tier 1 keys. Cross-tier isolation maintained.

#### Task 3.9: Implement Graceful Degradation
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Tasks 3.3-3.6

Add degradation logic across cache modules:
- Redis connection failure falls back to direct PostgreSQL
- Redis timeout falls back to direct PostgreSQL
- Merge error falls back to direct PostgreSQL result
- Circuit breaker pattern for repeated Redis failures
- Logging at each degradation level

**Verification:** Application serves correct data when Redis is unavailable. Latency increases but no errors.

### Phase 4: INTEGRATE -- Routes and API

#### Task 4.1: Rewrite Route Handlers for v2
**Priority:** P1
**Agent:** Build Agent
**Estimated Effort:** 4-5 hours
**Dependency:** Phase 3 complete

Rewrite `routes/table_routes.py`:
- Remove all formatting-related routes
- Remove all SQLAlchemy dependencies
- Add async ConnectionPool and Redis as FastAPI dependencies
- Integrate two-tier cache flow: check cache -> merge -> serve
- Integrate graceful degradation
- Update health check to include cache tier status

Rewrite `routes/models.py`:
- Remove FormattingRequest/FormattingResponse
- Remove CacheRequest/CacheResponse (cache is internal, not API-exposed)
- Update TableDataRequest to support two-tier cache parameters
- Add cache status to health check response

**Verification:** Routes serve data through two-tier cache. Fallback works when Redis unavailable.

#### Task 4.2: Update Demo Application
**Priority:** P2
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Task 4.1

Update `examples/demo/app.py`:
- Use ConnectionPool for database access
- Configure Redis connection for two-tier cache
- Demonstrate fund screener pattern with pagination
- Demonstrate cache hit vs. cache miss paths
- Show graceful degradation when Redis unavailable

**Verification:** Demo app runs end-to-end with two-tier caching.

### Phase 5: EVOLVE -- CSS Design Tokens

**Confirmed Design Decisions:**

| # | Topic | Decision |
|---|-------|----------|
| 1 | **Naming** | Hybrid B+C — ~30-40 public semantic tokens (`--tablix-color-primary`) + component fallbacks via `var(--tablix-search-radius, var(--tablix-radius))` |
| 2 | **Output modes** | Three modes: `themed` (full visual), `minimal` (structural + spacing), `headless` (structural only) |
| 3 | **CSS delivery** | Both static file helpers (`get_css_path()`, `get_css_link_tag()`) and FastAPI static mount. Auto-compile on startup. |
| 4 | **Precedence** | Single compiled source (YAML OR dict, not both — compiler enforces). CSS cascade always wins at runtime. Heavily commented output for AI assistants. |
| 5 | **Inline overrides** | Dict of selectors → CSS properties complements tokens. `style_mode` switch: `"inline"` (style block) or `"class"` (generated override classes). All elements have IDs. |
| 6 | **Dark mode** | Full `light-dark()` conversion. `color-scheme: light dark` on `:root`. No `@media (prefers-color-scheme)` blocks, no `.dark` class duplication. No a11y `@media` blocks (genX handles it). |

#### Task 5.1: Define Design Token Architecture
**Priority:** P2
**Agent:** Architect Agent
**Estimated Effort:** 4-5 hours
**Dependency:** Phase 4 complete

Design the complete token system:

**Naming convention (Hybrid B+C):**
- ~30-40 public semantic tokens as the contract: `--tablix-color-primary`, `--tablix-spacing-gap`, `--tablix-radius`, etc.
- Component-level tokens as opt-in overrides via CSS fallback: `var(--tablix-search-radius, var(--tablix-radius))`
- Component tokens only take effect if consumer explicitly sets them; otherwise fall through to semantic
- Token categories: colors, spacing, sizing, typography, borders/radius, shadows, transitions, z-index

**Token config schema (JSON/YAML):**
```yaml
colors:
  primary:
    light: "#3b82f6"
    dark: "#60a5fa"
  bg:
    light: "#ffffff"
    dark: "#1f2937"
  border:
    light: "#e5e7eb"
    dark: "#374151"
spacing:
  gap: "clamp(8px, calc(16 * var(--tablix-scale)), 24px)"
  padding: "clamp(12px, calc(24 * var(--tablix-scale)), 36px)"
radius:
  default: "6px"
# ... etc
```

**Python dict input format (TypedDict):**
```python
tokens: TokenInput = {
    "colors": {"primary": {"light": "#e11d48", "dark": "#fb7185"}},
    "spacing": {"gap": "12px"},
}
```

**Three output modes:**
- `themed`: Full CSS — structural + spacing + visual. Current filter_variables.css values as defaults. All colors as `light-dark()`.
- `minimal`: Structural + spacing — display, position, flex/grid, height, padding, gap, width. No colors, borders, shadows, radius, transitions.
- `headless`: Structural only — display, position, flex/grid, pointer-events. No spacing, no visual.

**Scope:** All components — table, filters, pagination, headers. Define complete token inventory for each.

**Class name contract:** Predictable `tablix-{component}-{element}` naming for external tooling and testing.

**Verification:** Token schema validates. All three modes produce valid CSS. Component fallback pattern works.

#### Task 5.2: Implement Design Token Compiler
**Priority:** P2
**Agent:** Build Agent
**Estimated Effort:** 5-6 hours
**Dependency:** Task 5.1

**Token compilation pipeline:**
- Frozen Pydantic models for token definitions (JSON/YAML → Pydantic → CSS)
- Single-source enforcement: compiler errors if both YAML file and Python dict are provided
- `compile_tokens(tokens: TokenConfig, mode: Literal["themed", "minimal", "headless"]) -> str` — generates CSS file content
- `compile_tokens_to_file(tokens, mode, output_path)` — writes generated .css file for static serving
- `compile_tokens_from_dict(token_dict: dict) -> str` — for programmatic use
- Default theme: current `filter_variables.css` values become the default `TokenConfig`

**light-dark() color conversion:**
- All color tokens emit `light-dark()` notation: `--tablix-color-primary: light-dark(#3b82f6, #60a5fa);`
- Generated CSS includes `color-scheme: light dark;` on `:root`
- No `@media (prefers-color-scheme)` blocks
- No `.dark` / `[data-theme="dark"]` class blocks
- No `@media (prefers-reduced-motion)` — genX handles a11y
- No `@media (prefers-contrast: high)` — genX handles a11y

**Heavily commented output:**
```css
/*
 * Tablix Design Tokens — Generated by declaro-tablix
 * Source: tokens.yaml
 * Mode: themed
 * Generated: 2026-02-16T15:30:00Z
 *
 * OVERRIDE GUIDE:
 * To override any token, redefine it in your own stylesheet
 * AFTER this file. CSS cascade ensures your value wins.
 *
 * For per-component overrides, set the component token:
 *   :root { --tablix-search-radius: 999px; }
 * It will take precedence over --tablix-radius for search inputs only.
 */
```

**Auto-compile on startup:**
- On first import or app boot, check if compiled .css exists and is fresh
- If stale or missing, recompile from token source
- Cache compiled output to avoid recompilation on subsequent imports

**CSS delivery helpers:**
- `get_css_path(mode) -> Path` — returns path to compiled CSS file
- `get_css_link_tag(mode) -> str` — returns `<link rel="stylesheet" href="...">` tag
- `get_tablix_static_mount() -> StaticFiles` — returns FastAPI StaticFiles mount

**Verification:** Compiler produces valid CSS in all three modes. `light-dark()` values resolve correctly. Auto-compile works on startup. Both delivery methods serve the CSS.

#### Task 5.3: Implement Inline Style Override System
**Priority:** P2
**Agent:** Build Agent
**Estimated Effort:** 3-4 hours
**Dependency:** Task 5.2

**Override API in render_table_ui():**
```python
html = render_table_ui(
    config=table_config,
    data=rows,
    styles={
        ".tablix-search-field": {"border-radius": "999px", "background": "#f0f0f0"},
        "#filter-security_type": {"width": "400px"},
        "#cell-aum-0": {"font-weight": "bold"},
    },
    style_mode="inline",  # or "class"
)
```

**Two output modes via `style_mode` switch:**
- `"inline"`: Emits a `<style>` block before the HTML with the override rules
- `"class"`: Generates named override classes (`tablix-override-{hash}`), writes them to the compiled CSS, applies classes to elements

**All elements must have IDs** for targetability (already supported via tablix's `*_id_override` and `*_id_template` fields).

**Verification:** Inline mode produces correct `<style>` block. Class mode generates and applies override classes. Selectors target correct elements by class and ID.

#### Task 5.4: Implement Introspection API
**Priority:** P2
**Agent:** Build Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Task 5.2

**Programmatic introspection surface:**
- `get_class_names() -> dict[str, ClassInfo]` — all CSS class names with descriptions, component, associated tokens
- `get_token_contract() -> dict[str, TokenInfo]` — all CSS custom properties with types, defaults (light + dark), descriptions
- `get_component_map() -> dict[str, list[str]]` — maps component names to their class names
- All return types are frozen Pydantic models for type safety

**Generated documentation manifest (JSON):**
- Machine-readable JSON file listing all tokens and class names
- For external tooling: linters, test selectors, documentation generators, AI coding assistants
- Auto-generated alongside compiled CSS

**Verification:** Introspection returns complete class name and token registries. JSON manifest validates against schema. All components (table, filters, pagination, headers) represented.

### Phase 6: TEST -- Comprehensive Test Suite

#### Task 6.1: Cache Key Generation Tests
**Priority:** P1
**Agent:** Test Agent
**Estimated Effort:** 1-2 hours
**Dependency:** Task 3.1

BDD scenarios:
- Deterministic key generation (same inputs -> same key)
- Key format correctness
- Hash collision resistance
- User scoping in Tier 2 keys

#### Task 6.2: Tier 1 Cache Tests
**Priority:** P1
**Agent:** Test Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Task 3.3

BDD scenarios:
- Fund cache warm loads all records
- Single fund lookup returns correct data
- MGET returns correct records for page
- Missing fund handled gracefully
- TTL expiry works correctly
- Invalidation deletes correct keys

#### Task 6.3: Tier 2 Cache Tests
**Priority:** P1
**Agent:** Test Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Task 3.4

BDD scenarios:
- Index created on first request (cache miss)
- Index served from cache on subsequent request (cache hit)
- Overlay created on first request
- TTL expiry causes re-fetch
- User write invalidates overlay only
- Different users have isolated caches

#### Task 6.4: Merge Engine Tests
**Priority:** P1
**Agent:** Test Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Tasks 3.5, 3.6

BDD scenarios:
- Screener pagination produces correct slices
- Fund records hydrated correctly from MGET
- Missing fund records filtered from results
- View merge attaches overlay to skeleton
- Skeleton not mutated by merge
- Missing overlay responses produce None values

#### Task 6.5: Graceful Degradation Tests
**Priority:** P1
**Agent:** Test Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Task 3.9

BDD scenarios:
- Redis unavailable: falls back to Postgres
- Redis timeout: falls back to Postgres
- Redis OOM: LRU eviction occurs
- Postgres unavailable: serves from cache
- Both unavailable: returns appropriate error

#### Task 6.6: Integration Tests
**Priority:** P1
**Agent:** Test Agent
**Estimated Effort:** 3-4 hours
**Dependency:** Phase 4 complete

End-to-end scenarios:
- Full request flow through two-tier cache
- Route handler -> cache check -> merge -> response
- Cache warm -> cache hit -> response
- Cache miss -> Postgres -> cache write -> response
- User write -> invalidation -> re-fetch

#### Task 6.7: Memory Budget Tests
**Priority:** P2
**Agent:** Test Agent
**Estimated Effort:** 2-3 hours
**Dependency:** Phase 3 complete

Memory scenarios:
- Tier 1 within 13MB budget
- Tier 2 per-user within 540KB budget
- 1000 simulated users within 512MB Redis budget
- LRU eviction prioritizes Tier 2 over Tier 1

---

## 5. Dependency Graph

```
Phase 0: Pre-Flight
    |
Phase 1: REMOVAL
    |-- Task 1.1: Remove Formatting
    |-- Task 1.2: Remove Old Caching
    |-- Task 1.3: Tag SQLAlchemy Usage
    |
Phase 2: MIGRATE (depends on Phase 1)
    |-- Task 2.1: Convert Customization Models
    |-- Task 2.2: Convert table_data_repository (depends on 2.1)
    |-- Task 2.3: Convert table_config_repository (depends on 2.1)
    |-- Task 2.4: Convert customization/persistence (depends on 2.1)
    |-- Task 2.5: Convert remaining repositories (depends on 2.1)
    |-- Task 2.6: Remove SQLAlchemy dependency (depends on 2.1-2.5)
    |
Phase 3: BUILD (depends on Phase 2)
    |-- Task 3.1: Cache Key Generator (no deps within phase)
    |-- Task 3.2: Cache Configuration (depends on 3.1)
    |-- Task 3.3: Tier 1 Manager (depends on 3.1, 3.2)
    |-- Task 3.4: Tier 2 Manager (depends on 3.1, 3.2)
    |-- Task 3.5: Merge Engine - Screener (depends on 3.3, 3.4)
    |-- Task 3.6: Merge Engine - Views (depends on 3.3, 3.4)
    |-- Task 3.7: Cache Warmer (depends on 3.3, 3.4)
    |-- Task 3.8: Invalidation Handler (depends on 3.3, 3.4)
    |-- Task 3.9: Graceful Degradation (depends on 3.3-3.6)
    |
Phase 4: INTEGRATE (depends on Phase 3)
    |-- Task 4.1: Rewrite Routes (depends on Phase 3)
    |-- Task 4.2: Update Demo App (depends on 4.1)
    |
Phase 5: EVOLVE (depends on Phase 4)
    |-- Task 5.1: Define Token Architecture
    |-- Task 5.2: Implement Token Compiler + light-dark() + auto-compile (depends on 5.1)
    |-- Task 5.3: Implement Inline Style Override System (depends on 5.2)
    |-- Task 5.4: Implement Introspection API + JSON manifest (depends on 5.2)
    |       (5.3 and 5.4 can run in parallel)
    |
Phase 6: TEST (parallel with Phases 3-5)
    |-- Tasks 6.1-6.7: Tests written alongside implementation
```

---

## 6. Risk Assessment

### 6.1 Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Removing formatting breaks template rendering | Low | High | Templates use fx-* attributes (template-level), not Python formatters |
| SQLAlchemy removal breaks unexplored import chains | Medium | Medium | Task 1.3 maps all imports before removal begins |
| Persistum query API gaps vs. raw SQL | Low | Medium | persistum supports raw() for complex queries; demo app already works |
| Redis connection management complexity | Medium | Medium | Graceful degradation means Redis failure is performance, not functional issue |
| Merge engine bugs produce incorrect data | Low | High | Comprehensive unit tests on pure merge functions |

### 6.2 Schedule Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Phase 2 (persistum migration) takes longer than expected | Medium | High | Can be parallelized across repositories; table_repository.py is already done |
| Phase 3 (cache build) has unforeseen complexity | Low | Medium | v2 cache architecture spec is comprehensive; code examples provided |
| Test coverage gaps discovered late | Medium | Medium | Test agent writes tests alongside implementation (Phase 6 parallel) |

### 6.3 Rollback Strategy

Each phase is independently reversible:
- **Phase 1:** `git revert` to restore deleted files
- **Phase 2:** Can maintain SQLAlchemy dependency as fallback during migration
- **Phase 3:** New cache modules are additive; old code already removed in Phase 1
- **Phase 4:** Routes can fall back to direct DB if cache layer has issues
- **Phase 5:** CSS tokens are additive to existing CSS variable system

---

## 7. Compliance Verification

### 7.1 Code Style Compliance

Every new module must pass these checks:

- [ ] No classes except Pydantic models (frozen=True), Protocol, Exception
- [ ] All functions have complete type hints
- [ ] No global mutable state
- [ ] All I/O operations are async
- [ ] All data structures are immutable (frozenset, tuple, frozen dataclass/Pydantic)
- [ ] Pure functions with explicit dependencies (no hidden state)
- [ ] Set theory operations use frozenset where applicable

### 7.2 Architecture Compliance

- [ ] All database access via declaro-persistum ConnectionPool
- [ ] All number formatting via genX fmtx.js (no Python formatting)
- [ ] Two-tier cache with independent invalidation clocks
- [ ] Merge engine as pure functions (no DB access, no side effects)
- [ ] Graceful degradation (Redis failure -> Postgres fallback)
- [ ] Deterministic cache key generation (centralized, no ad-hoc keys)

### 7.3 Performance Compliance (from v2 spec)

- [ ] Cache hit latency: <2ms
- [ ] Cache miss latency: <200ms
- [ ] Tier 1 memory: <13MB fixed
- [ ] Tier 2 memory: <540KB per user
- [ ] 1000 users total: <512MB Redis

---

## 8. Estimated Timeline

| Phase | Duration | Parallelism |
|-------|----------|-------------|
| Phase 0: Pre-Flight | 1 hour | Sequential |
| Phase 1: Removal | 4-6 hours | Tasks 1.1 + 1.2 parallel |
| Phase 2: Migration | 12-16 hours | Tasks 2.2-2.5 parallel after 2.1 |
| Phase 3: Cache Build | 20-28 hours | Tasks 3.3 + 3.4 parallel; 3.5 + 3.6 parallel |
| Phase 4: Integration | 6-8 hours | Sequential |
| Phase 5: CSS Tokens | 14-18 hours | Tasks 5.3 + 5.4 parallel after 5.2 |
| Phase 6: Testing | 14-20 hours | Parallel with Phases 3-5 |
| **Total** | **62-86 hours** | With parallelism: **~40-55 hours** |

---

## 9. Agent Assignment

### Build Agent Responsibilities
- All file creation and modification
- Code implementation following specifications
- Running tests after each task
- Reporting blockers to Architect Agent

### Test Agent Responsibilities
- Writing BDD scenarios (Gherkin)
- Implementing test fixtures and step definitions
- Running test suites and reporting coverage
- Memory budget and performance tests

### Architect Agent Responsibilities
- Task 1.3 (SQLAlchemy usage mapping)
- Task 5.1 (Design token system design)
- Code review for architecture compliance
- Resolving design decisions that arise during implementation
- Veto power on architectural violations
