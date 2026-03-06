@bdd
Feature: Sync Connection Pool Auto-Migration
  As a developer using declaro_persistum
  I want SyncConnectionPool to automatically apply schema migrations
  So that my database schema stays in sync with my Pydantic models

  Background:
    Given a temporary database file

  # =============================================================================
  # Core Auto-Migration Scenarios
  # =============================================================================

  @precommit @sqlite
  Scenario: Sync SQLite pool creates missing tables on first connection
    Given a Pydantic model "Task" with fields:
      | field     | type   | primary_key |
      | task_id   | str    | true        |
      | title     | str    | false       |
      | completed | int    | false       |
    When I create a sync SQLite pool with auto_migrate enabled
    And I acquire a connection from the sync pool
    Then the "tasks" table should exist in the database
    And the "tasks" table should have columns:
      | column    | type    |
      | task_id   | TEXT    |
      | title     | TEXT    |
      | completed | INTEGER |

  @precommit @sqlite
  Scenario: Sync SQLite pool adds new columns to existing tables
    Given a database with existing table "tasks":
      | column  | type |
      | task_id | TEXT |
      | title   | TEXT |
    And a Pydantic model "Task" with fields:
      | field       | type   | primary_key |
      | task_id     | str    | true        |
      | title       | str    | false       |
      | description | str    | false       |
    When I create a sync SQLite pool with auto_migrate enabled
    And I acquire a connection from the sync pool
    Then the "tasks" table should have columns:
      | column      | type |
      | task_id     | TEXT |
      | title       | TEXT |
      | description | TEXT |

  @precommit @turso
  Scenario: Sync Turso pool creates missing tables on first connection
    Given a Pydantic model "Task" with fields:
      | field     | type   | primary_key |
      | task_id   | str    | true        |
      | title     | str    | false       |
      | completed | int    | false       |
    When I create a sync Turso pool with auto_migrate enabled
    And I acquire a connection from the sync pool
    Then the "tasks" table should exist in the database
    And the "tasks" table should have columns:
      | column    | type    |
      | task_id   | TEXT    |
      | title     | TEXT    |
      | completed | INTEGER |

  # =============================================================================
  # Enum/Literal Type Handling
  # =============================================================================

  @precommit @sqlite
  Scenario: Sync pool expands Literal types to lookup tables
    Given a Pydantic model "Order" with fields:
      | field    | type                              | primary_key |
      | order_id | str                               | true        |
      | status   | Literal["pending", "shipped"]     | false       |
    When I create a sync SQLite pool with auto_migrate enabled
    And I acquire a connection from the sync pool
    Then the "_dp_enum_orders_status" lookup table should exist
    And the "_dp_enum_orders_status" table should contain values:
      | value    |
      | pending  |
      | shipped  |
    And the "orders" table should have a foreign key to "_dp_enum_orders_status"

  # =============================================================================
  # Migration Disabled Scenarios
  # =============================================================================

  @precommit @sqlite
  Scenario: Sync pool skips migration when auto_migrate is disabled
    Given a Pydantic model "Task" with fields:
      | field   | type | primary_key |
      | task_id | str  | true        |
    When I create a sync SQLite pool with auto_migrate disabled
    And I acquire a connection from the sync pool
    Then the "tasks" table should not exist in the database

  # =============================================================================
  # Error Handling
  # =============================================================================

  @precommit @sqlite
  Scenario: Sync pool continues on migration failure with graceful degradation
    Given a database with existing table "tasks" containing data:
      | task_id | title      |
      | 1       | First task |
    And a Pydantic model "Task" with incompatible schema change
    When I create a sync SQLite pool with auto_migrate enabled
    Then the pool should log a migration warning
    And the existing data should remain intact

  # =============================================================================
  # Parity with Async Pool
  # =============================================================================

  @precommit @sqlite
  Scenario: Sync pool produces identical schema to async pool
    Given a Pydantic model "Task" with fields:
      | field       | type   | primary_key |
      | task_id     | str    | true        |
      | title       | str    | false       |
      | completed   | int    | false       |
      | description | str    | false       |
    When I create an async SQLite pool with auto_migrate enabled
    And I create a sync SQLite pool with auto_migrate enabled on a separate database
    Then both databases should have identical schemas
