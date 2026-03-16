@bdd
Feature: Materialized View Emulation for SQLite/Turso
  As a developer using declaro_persistum
  I want materialized views to work on SQLite/Turso
  So that I can cache expensive queries without changing my schema definition

  Background:
    Given a schema with an "orders" table
    And the orders table has columns: id, user_id, total, created_at

  # =============================================================================
  # Create Emulated Materialized View
  # =============================================================================

  @precommit
  Scenario: Create emulated materialized view with manual refresh
    Given a view definition with materialized=true and refresh="manual"
    And the view query is "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id"
    When the SQLite applier generates CREATE SQL
    Then it should create metadata table "_dp_materialized_views"
    And it should create backing table "monthly_stats"
    And it should register the view in metadata with refresh_strategy="manual"

  @precommit
  Scenario: Create emulated materialized view with depends_on
    Given a view definition with materialized=true
    And the view has depends_on=["orders"]
    When the SQLite applier generates CREATE SQL
    Then the metadata should include depends_on as JSON array '["orders"]'

  # =============================================================================
  # Refresh Emulated Materialized View
  # =============================================================================

  @precommit
  Scenario: Refresh emulated materialized view atomically
    Given an existing emulated materialized view "monthly_stats"
    And the view query is "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
    When refresh_matview_sql() is called with atomic=true
    Then it should generate DELETE FROM "monthly_stats"
    And it should generate INSERT INTO "monthly_stats" with the query
    And it should UPDATE last_refreshed_at in metadata

  @precommit
  Scenario: Refresh emulated materialized view non-atomically
    Given an existing emulated materialized view "monthly_stats"
    When refresh_matview_sql() is called with atomic=false
    Then it should generate DROP TABLE "monthly_stats"
    And it should generate CREATE TABLE "monthly_stats" AS query

  # =============================================================================
  # Trigger-Based Refresh
  # =============================================================================

  @precommit
  Scenario: Create materialized view with trigger-based refresh
    Given a view definition with refresh="trigger"
    And trigger_sources=["orders"]
    When the SQLite applier generates CREATE SQL
    Then it should create trigger "_dp_refresh_monthly_stats_on_orders_insert"
    And it should create trigger "_dp_refresh_monthly_stats_on_orders_update"
    And it should create trigger "_dp_refresh_monthly_stats_on_orders_delete"

  @precommit
  Scenario: Trigger fires AFTER INSERT on source table
    Given trigger SQL for matview "monthly_stats" on source "orders"
    When I examine the INSERT trigger
    Then it should be AFTER INSERT ON "orders"
    And the trigger body should DELETE and INSERT the matview

  @precommit
  Scenario: Trigger fires AFTER UPDATE on source table
    Given trigger SQL for matview "monthly_stats" on source "orders"
    When I examine the UPDATE trigger
    Then it should be AFTER UPDATE ON "orders"

  @precommit
  Scenario: Trigger fires AFTER DELETE on source table
    Given trigger SQL for matview "monthly_stats" on source "orders"
    When I examine the DELETE trigger
    Then it should be AFTER DELETE ON "orders"

  # =============================================================================
  # Drop Emulated Materialized View
  # =============================================================================

  @precommit
  Scenario: Drop emulated materialized view
    Given an existing emulated materialized view "monthly_stats"
    When drop_matview_sql() is called
    Then it should generate DROP TABLE IF EXISTS "monthly_stats"
    And it should generate DELETE FROM _dp_materialized_views WHERE name='monthly_stats'

  @precommit
  Scenario: Drop emulated materialized view with triggers
    Given an existing emulated materialized view "monthly_stats" with trigger_sources=["orders"]
    When drop_matview_sql() is called with trigger cleanup
    Then it should generate DROP TRIGGER for insert trigger
    And it should generate DROP TRIGGER for update trigger
    And it should generate DROP TRIGGER for delete trigger

  # =============================================================================
  # Introspect Emulated Materialized Views
  # =============================================================================

  @precommit
  Scenario: Introspect detects emulated materialized views
    Given a database with _dp_materialized_views containing "monthly_stats"
    When the SQLite inspector introspects views
    Then it should return "monthly_stats" with materialized=true

  @precommit
  Scenario: Introspect returns refresh strategy from metadata
    Given a database with "monthly_stats" having refresh_strategy="trigger"
    When the SQLite inspector introspects views
    Then it should return refresh="trigger" for "monthly_stats"

  @precommit
  Scenario: Introspect returns depends_on from metadata
    Given a database with "monthly_stats" having depends_on='["orders"]'
    When the SQLite inspector introspects views
    Then it should return depends_on=["orders"] for "monthly_stats"

  @precommit
  Scenario: Introspect handles missing metadata table gracefully
    Given a database without _dp_materialized_views table
    When the SQLite inspector introspects views
    Then it should return only regular views
    And it should not raise an error

  # =============================================================================
  # Validation
  # =============================================================================

  @precommit
  Scenario: Validate trigger_sources requires trigger or hybrid refresh
    Given a view definition with trigger_sources=["orders"]
    And refresh="manual"
    When validate_view() is called
    Then it should raise ValueError "trigger_sources requires refresh='trigger' or 'hybrid'"

  @precommit
  Scenario: Accept valid SQLite refresh strategies
    Given a view definition with refresh="manual"
    When validate_view() is called
    Then it should not raise an error

  @precommit
  Scenario: Accept trigger refresh strategy
    Given a view definition with refresh="trigger"
    And trigger_sources=["orders"]
    When validate_view() is called
    Then it should not raise an error

  @precommit
  Scenario: Accept hybrid refresh strategy
    Given a view definition with refresh="hybrid"
    When validate_view() is called
    Then it should not raise an error

  # =============================================================================
  # Dialect Compatibility
  # =============================================================================

  @precommit
  Scenario: SQLite and Turso generate identical SQL
    Given a view definition with materialized=true
    When SQLite applier generates CREATE SQL
    And Turso applier generates CREATE SQL
    Then both should produce identical SQL

  @precommit
  Scenario: PostgreSQL uses native materialized views
    Given a view definition with materialized=true
    When PostgreSQL applier generates CREATE SQL
    Then it should use CREATE MATERIALIZED VIEW (not table emulation)
