@bdd
Feature: DELETE Query Building
  As a developer using declaro_persistum
  I want to build DELETE queries with a fluent API
  So that I can safely remove data from the database

  Background:
    Given a schema with a "todos" table

  # =============================================================================
  # Pre-commit Tests (Fast, Mocked)
  # =============================================================================

  @precommit
  Scenario: Basic DELETE query
    When I build a DELETE query
    Then the SQL should contain "DELETE FROM todos"

  @precommit
  Scenario: DELETE with WHERE clause
    When I build a DELETE WHERE id = "abc-123"
    Then the SQL should contain "DELETE FROM todos"
    And the SQL should contain "WHERE"
    And the SQL should contain "todos.id"
