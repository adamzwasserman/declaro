# Epic: Complete tablix with tableV2 Infrastructure + Persistum Integration

**Epic ID:** `react2htmx2-pqh`
**Priority:** P1
**Status:** Open

## Overview

tablix was supposed to be a renamed copy of `/Users/adam/dev/buckler/idd/backend/services/tableV2` but is missing major components. This epic completes tablix by copying the missing infrastructure and integrating with declaro-persistum.

## Current State

**What tablix has:**
- `domain/models.py` - Domain models (SortDefinition, TableConfig, etc.)
- `models.py` - Database models for persistum
- `templates/` - Jinja2 templates (but NO sort buttons)

**What tablix is MISSING:**
- `routes/` - No route handlers
- `services/` - No table service with sorting logic
- `caching/` - No per-user cache infrastructure
- `repositories/` - No data repository

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FastAPI Routes                          │
│                    (routes/table_routes.py)                     │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Table Service                            │
│                   (services/table_service.py)                   │
│         - sort_table_data()                                     │
│         - get_table_data()                                      │
│         - build_table_state()                                   │
└─────────────────────────────────────────────────────────────────┘
                    │                       │
                    ▼                       ▼
┌───────────────────────────┐   ┌───────────────────────────────┐
│    Persistum Repository   │   │      Redis Cache Layer        │
│ (repositories/table_repo) │   │   (caching/cache_service)     │
│                           │   │                               │
│ - TableConfig storage     │   │ - Per-user sorted data        │
│ - User preferences        │   │ - Query result caching        │
│ - Column configs          │   │ - TTL management              │
└───────────────────────────┘   └───────────────────────────────┘
            │                               │
            ▼                               ▼
┌───────────────────────────┐   ┌───────────────────────────────┐
│    declaro-persistum      │   │           Redis               │
│   (SQLite/PostgreSQL)     │   │                               │
└───────────────────────────┘   └───────────────────────────────┘
```

## Execution Order

| Phase | Issue ID | Feature | Dependencies |
|-------|----------|---------|--------------|
| 1 | `react2htmx2-0bz` | Persistum Repository Layer | None (READY) |
| 1 | `react2htmx2-88i` | HTMX Sort Buttons Template | None (READY) |
| 2 | `react2htmx2-772` | Table Service with Sorting | Repository |
| 3 | `react2htmx2-rsb` | FastAPI Routes for Table Data | Service |
| 3 | `react2htmx2-8jz` | Redis Caching Layer | Service |
| 4 | `react2htmx2-h8s` | Converter Uses tablix Routes | Routes, Template |
| 5 | `react2htmx2-e2q` | Documentation Update | Converter |

---

## Feature 1: Persistum Repository Layer

**Issue ID:** `react2htmx2-0bz`

### Gherkin Specification

```gherkin
Feature: Persistum-backed table repository
  As a tablix user
  I want table configs stored via persistum
  So that data persists across sessions

  Scenario: Get table config by name
    Given a table config "holdings" exists in the database
    When I call repository.get_config(conn, "holdings")
    Then I receive a TableConfigModel with table_name="holdings"

  Scenario: Save new table config
    Given no table config "new_table" exists
    When I call repository.save_config(conn, config)
    Then the config is persisted to the database
    And I can retrieve it by name

  Scenario: Get user preferences
    Given user "user123" has preferences for table "holdings"
    When I call repository.get_user_preferences(conn, "user123", "holdings_id")
    Then I receive a list of UserFilterPreference objects
```

### BDD Workflow

1. Write Gherkin spec (above)
2. Write failing test in `tests/test_repository.py`
3. Run test (RED)
4. Implement `repositories/table_repository.py` using persistum
5. Run test (GREEN)
6. Refactor if needed

### Files

- Create: `src/declaro_tablix/repositories/__init__.py`
- Create: `src/declaro_tablix/repositories/table_repository.py`
- Create: `tests/test_repository.py`

---

## Feature 2: Table Service with Sorting

**Issue ID:** `react2htmx2-772`

### Gherkin Specification

```gherkin
Feature: Table service with sorting support
  As a tablix user
  I want to sort table data by columns
  So that I can view data in my preferred order

  Scenario: Sort data ascending by single column
    Given table data with columns [name, value]
    And rows [("zebra", 1), ("apple", 2), ("mango", 3)]
    When I call sort_table_data(data, [SortDefinition(column_id="name", direction="asc")])
    Then rows are ordered [("apple", 2), ("mango", 3), ("zebra", 1)]

  Scenario: Sort data descending
    Given table data with column "value"
    When I sort by "value" descending
    Then rows are ordered by value high to low

  Scenario: Multi-column sort with priority
    Given sort definitions with priorities [0, 1]
    When I apply sorting
    Then priority 0 column is primary sort
    And priority 1 column breaks ties

  Scenario: Get table data with sorting
    Given a table "holdings" with data
    When I call get_table_data(conn, "holdings", sorts=[...])
    Then data is returned sorted
    And result is cached per user
```

### BDD Workflow

1. Write Gherkin spec (above)
2. Write failing tests in `tests/test_table_service.py`
3. Run tests (RED)
4. Copy `services/table_service.py` from tableV2, modify to use persistum
5. Run tests (GREEN)

### Files

- Create: `src/declaro_tablix/services/__init__.py`
- Create: `src/declaro_tablix/services/table_service.py`
- Create: `tests/test_table_service.py`

---

## Feature 3: FastAPI Routes for Table Data

**Issue ID:** `react2htmx2-rsb`

### Gherkin Specification

```gherkin
Feature: FastAPI routes for table operations
  As an API consumer
  I want REST endpoints for table data
  So that I can fetch sorted/filtered data

  Scenario: POST /tables/data returns sorted data
    Given table "holdings" exists with data
    When I POST to /tables/data with {"table_name": "holdings", "sort_column": "name", "sort_direction": "asc"}
    Then I receive 200 OK
    And response contains sorted rows
    And response matches TableDataResponse schema

  Scenario: Sort direction parameter
    When I POST with sort_direction="desc"
    Then data is sorted descending

  Scenario: Pagination with sorting
    Given 100 rows in table
    When I POST with page=2, per_page=25, sort_column="name"
    Then I receive rows 26-50 sorted by name

  Scenario: HTMX partial response
    When request has HX-Request header
    Then response is HTML partial (not JSON)
```

### BDD Workflow

1. Write Gherkin spec (above)
2. Write failing tests in `tests/test_routes.py`
3. Run tests (RED)
4. Copy `routes/table_routes.py` from tableV2, adapt for tablix
5. Run tests (GREEN)

### Files

- Create: `src/declaro_tablix/routes/__init__.py`
- Create: `src/declaro_tablix/routes/table_routes.py`
- Create: `src/declaro_tablix/routes/models.py`
- Create: `tests/test_routes.py`

---

## Feature 4: Redis Caching Layer

**Issue ID:** `react2htmx2-8jz`

### Gherkin Specification

```gherkin
Feature: Redis caching for sorted table data
  As a tablix user
  I want sorted data cached per user
  So that repeated requests are fast

  Scenario: Cache sorted data with TTL
    Given user "user123" requests sorted data for "holdings"
    When data is fetched and sorted
    Then result is cached with key "table_data:holdings:user123:sort_hash"
    And cache TTL is 300 seconds

  Scenario: Return cached data on repeat request
    Given cached data exists for user's query
    When same request is made within TTL
    Then cached data is returned
    And database is not queried

  Scenario: Cache invalidation on data change
    Given cached data exists for "holdings"
    When holdings data is modified
    Then cache keys matching "table_data:*holdings*" are invalidated

  Scenario: Per-user cache isolation
    Given user A and user B both request "holdings" with different sorts
    Then each user has separate cache entries
    And user A's cache doesn't affect user B
```

### BDD Workflow

1. Write Gherkin spec (above)
2. Write failing tests in `tests/test_caching.py`
3. Run tests (RED)
4. Copy caching modules from tableV2
5. Run tests (GREEN)

### Files

- Create: `src/declaro_tablix/caching/__init__.py`
- Create: `src/declaro_tablix/caching/cache_service.py`
- Create: `src/declaro_tablix/caching/cache_repository.py`
- Create: `src/declaro_tablix/caching/cache_middleware.py`
- Create: `tests/test_caching.py`

---

## Feature 5: HTMX Sort Buttons in Table Template

**Issue ID:** `react2htmx2-88i`

### Gherkin Specification

```gherkin
Feature: HTMX-powered sort buttons in table headers
  As a user viewing a table
  I want to click column headers to sort
  So that I can reorder data without page reload

  Scenario: Sortable column renders button
    Given a column with sortable=True
    When table template is rendered
    Then header contains a button with hx-get attribute
    And button has class "tablix-sort-btn"

  Scenario: Non-sortable column renders plain text
    Given a column with sortable=False
    When table template is rendered
    Then header contains plain text (no button)

  Scenario: Active sort shows indicator
    Given sort_field="name" and sort_dir="asc"
    When column "name" header is rendered
    Then button has class "tablix-sort-active"
    And button shows indicator

  Scenario: Click toggles sort direction
    Given current sort is "name" ascending
    When button hx-get is generated
    Then URL contains "?sort=name&dir=desc"

  Scenario: HTMX swaps table on click
    Given sort button with hx-target="closest table"
    When button is clicked
    Then only table element is replaced
    And page does not fully reload
```

### BDD Workflow

1. Write Gherkin spec (above)
2. Write failing template test in `tests/test_table_template.py`
3. Run test (RED)
4. Update `templates/components/table.html`
5. Run test (GREEN)

### Files

- Edit: `src/declaro_tablix/templates/components/table.html`
- Create: `tests/test_table_template.py`

---

## Feature 6: Converter Uses tablix Routes

**Issue ID:** `react2htmx2-h8s`

### Gherkin Specification

```gherkin
Feature: Converter generates apps using tablix routes
  As a react2htmx user
  I want converted apps to use tablix for tables
  So that sorting works out of the box

  Scenario: Generated main.py mounts tablix router
    Given a React app with tables
    When converter runs
    Then main.py includes "from declaro_tablix.routes import table_router"
    And main.py includes "app.include_router(table_router, prefix='/tables')"

  Scenario: Generated templates use tablix macros
    Given a React component with a sortable table
    When converter generates template
    Then template imports tablix table macro
    And template passes sort_field, sort_dir to macro

  Scenario: Table sort buttons work end-to-end
    Given converted HTMX app is running
    When I click a column header
    Then table data is sorted
    And only table partial is swapped (HTMX)
```

### BDD Workflow

1. Write Gherkin spec (above)
2. Write failing test in react2htmx `tests/test_tablix_integration.py`
3. Run test (RED)
4. Update `pipelines/route.py` to mount tablix router
5. Update `pipelines/template.py` to use tablix macros
6. Run test (GREEN)

### Files (react2htmx)

- Edit: `src/react2htmx/pipelines/route.py`
- Edit: `src/react2htmx/pipelines/template.py`
- Create: `tests/test_tablix_integration.py`

---

## Task: Documentation Update

**Issue ID:** `react2htmx2-e2q`

### Documentation Updates

After all features are complete, update documentation:

#### 1. tablix README.md
- Add "Sorting" section explaining:
  - How to enable sorting on columns (sortable=True, default)
  - How sort buttons work with HTMX
  - Per-user sort state caching

#### 2. tablix API Reference
- Document new modules:
  - `routes/` - FastAPI router and endpoints
  - `services/` - Table service functions
  - `caching/` - Redis cache layer
  - `repositories/` - Persistum-backed data access

#### 3. Integration Guide
- How to mount tablix router in FastAPI app
- How to configure Redis connection
- How to configure persistum connection

#### 4. react2htmx Converter Docs
- Update to mention tablix integration
- Document that sorting is automatic for converted tables

### Files

- Edit: `/Users/adam/dev/declaro/packages/declaro-tablix/README.md`
- Create: `/Users/adam/dev/declaro/packages/declaro-tablix/docs/api.md` (if needed)
- Edit: `/Users/adam/dev/react2htmx2/README.md`

---

## Source Reference

**tableV2 location:** `/Users/adam/dev/buckler/idd/backend/services/tableV2`

Key files to copy and adapt:
- `routes/table_routes.py` - FastAPI endpoints
- `services/table_service.py` - Sorting logic, per-user state
- `caching/cache_service.py` - Redis per-user cache
- `caching/cache_middleware.py` - HTTP-layer caching
- `repositories/table_data_repository.py` - Data access (replace with persistum)

## Integration Notes

**persistum handles (all persistent data):**
- TableConfig storage/retrieval
- User preferences (UserFilterPreference, UserSavedLayout)
- Column configs, filter configs
- Filter enum values

**Redis handles (ephemeral cache):**
- Per-user sorted data copies with TTL
- Query result caching
- HTTP response caching
