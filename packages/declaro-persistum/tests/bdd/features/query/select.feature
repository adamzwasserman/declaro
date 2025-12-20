@bdd
Feature: SELECT Query Building
  As a developer using declaro_persistum
  I want to build SELECT queries with a fluent API
  So that I can retrieve data safely and efficiently

  Background:
    Given a schema with a "users" table
    And the users table has columns: id, email, name, created_at, status, age

  # =============================================================================
  # Pre-commit Tests (Fast, Mocked)
  # =============================================================================

  @precommit
  Scenario: Basic SELECT all columns
    When I build a select query for all columns
    Then the SQL should be "SELECT * FROM users"
    And there should be no parameters

  @precommit
  Scenario: SELECT with specific columns
    When I select columns: id, email
    Then the SQL should contain "users.id"
    And the SQL should contain "users.email"

  @precommit
  Scenario: SELECT with WHERE equals condition
    When I select all columns
    And I add a WHERE condition: status equals "active"
    Then the SQL should contain "WHERE"
    And the SQL should contain "users.status"

  @precommit
  Scenario: SELECT with WHERE greater than condition
    When I select all columns
    And I add a WHERE condition: age > "18"
    Then the SQL should contain "WHERE"
    And the SQL should contain ">"

  @precommit
  Scenario: SELECT with ORDER BY ascending
    When I select all columns
    And I add ORDER BY created_at asc
    Then the SQL should contain "ORDER BY"
    And the SQL should contain "ASC"

  @precommit
  Scenario: SELECT with ORDER BY descending
    When I select all columns
    And I add ORDER BY created_at desc
    Then the SQL should contain "ORDER BY"
    And the SQL should contain "DESC"

  @precommit
  Scenario: SELECT with LIMIT
    When I select all columns
    And I add LIMIT 10
    Then the SQL should contain "LIMIT 10"

  @precommit
  Scenario: SELECT with OFFSET
    When I select all columns
    And I add LIMIT 10
    And I add OFFSET 5
    Then the SQL should contain "LIMIT 10"
    And the SQL should contain "OFFSET 5"

  @precommit
  Scenario: SELECT with COUNT aggregation
    When I select COUNT(*)
    Then the SQL should contain "COUNT(*)"

  @precommit
  Scenario: SELECT with SUM aggregation
    When I select SUM(age)
    Then the SQL should contain "SUM"

  @precommit
  Scenario: SELECT with GROUP BY
    When I select COUNT(*)
    And I add GROUP BY status
    Then the SQL should contain "GROUP BY"
    And the SQL should contain "users.status"
