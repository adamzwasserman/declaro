@bdd
Feature: Multi-Database Backend Support
  As a developer
  I want the same code to work on SQLite, PostgreSQL, and Turso
  So that I can choose my database without rewriting queries

  # =============================================================================
  # Stress Tests (Require Real Databases)
  # =============================================================================

  @stress @sqlite
  Scenario: Full CRUD cycle on SQLite
    Given an SQLite database connection
    And an empty todos table
    When I insert a todo with title "Test SQLite"
    And I update the todo to completed
    And I query for completed todos
    Then I should find 1 completed todo
    When I delete the todo
    Then the table should be empty

  @stress @postgresql
  Scenario: Full CRUD cycle on PostgreSQL
    Given a PostgreSQL database connection
    And an empty todos table
    When I insert a todo with title "Test PostgreSQL"
    And I update the todo to completed
    And I query for completed todos
    Then I should find 1 completed todo
    When I delete the todo
    Then the table should be empty

  @stress @turso
  Scenario: Full CRUD cycle on Turso
    Given a Turso database connection
    And an empty todos table
    When I insert a todo with title "Test Turso"
    And I update the todo to completed
    And I query for completed todos
    Then I should find 1 completed todo
    When I delete the todo
    Then the table should be empty

  @stress @sqlite
  Scenario: Large dataset on SQLite
    Given an SQLite database connection
    And a todos table with 1000 rows
    When I execute "SELECT COUNT(*) as count FROM todos"
    Then I should find exactly 1 result

  @stress @postgresql
  Scenario: Large dataset on PostgreSQL
    Given a PostgreSQL database connection
    And a todos table with 1000 rows
    When I execute "SELECT COUNT(*) as count FROM todos"
    Then I should find exactly 1 result
