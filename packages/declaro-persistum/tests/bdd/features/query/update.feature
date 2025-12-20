@bdd
Feature: UPDATE Query Building
  As a developer using declaro_persistum
  I want to build UPDATE queries with a fluent API
  So that I can safely modify data in the database

  Background:
    Given a schema with a "todos" table

  # =============================================================================
  # Pre-commit Tests (Fast, Mocked)
  # =============================================================================

  @precommit
  Scenario: Basic UPDATE with SET clause
    When I build an UPDATE setting completed = "true"
    Then the SQL should contain "UPDATE todos"
    And the SQL should contain "SET"
    And the SQL should contain "completed"

  @precommit
  Scenario: UPDATE with WHERE clause
    When I build an UPDATE setting completed = "true"
    And I add WHERE id = "abc-123"
    Then the SQL should contain "UPDATE todos"
    And the SQL should contain "SET"
    And the SQL should contain "WHERE"
    And the SQL should contain "todos.id"
