@bdd
Feature: INSERT Query Building
  As a developer using declaro_persistum
  I want to build INSERT queries with a fluent API
  So that I can safely insert data into the database

  Background:
    Given a schema with a "todos" table

  # =============================================================================
  # Pre-commit Tests (Fast, Mocked)
  # =============================================================================

  @precommit
  Scenario: Basic INSERT with single column
    When I build an INSERT with title = "Buy groceries"
    Then the SQL should contain "INSERT INTO todos"
    And the SQL should contain "title"

  @precommit
  Scenario: INSERT with multiple values
    When I build an INSERT with multiple values
    Then the SQL should contain "INSERT INTO todos"
    And the SQL should contain "title"
    And the SQL should contain "completed"

  @precommit
  Scenario: INSERT with RETURNING clause
    When I build an INSERT with title = "Test todo"
    And I add RETURNING clause for id
    Then the SQL should contain "RETURNING"
    And the SQL should contain "id"
