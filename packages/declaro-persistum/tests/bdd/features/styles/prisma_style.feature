@bdd
Feature: Prisma-Style Query API
  As a Prisma developer
  I want to use familiar Prisma patterns
  So that I can be productive immediately

  Background:
    Given a users table with Prisma-style interface
    And the users table has columns: id, email, name, status, age, created_at

  # =============================================================================
  # Pre-commit Tests (Fast, Mocked)
  # =============================================================================

  @precommit
  Scenario: Basic find_many
    When I call users.prisma.find_many()
    Then the SQL should contain "SELECT"
    And the SQL should contain "FROM users"

  @precommit
  Scenario: find_many with where clause
    When I call users.prisma.find_many(where={status: "active"})
    Then the SQL should contain "WHERE"
    And the SQL should contain "status"

  @precommit
  Scenario: find_many with gt operator
    When I call users.prisma.find_many(where={age: {"gt": 18}})
    Then the SQL should contain "WHERE"
    And the SQL should contain ">"

  @precommit
  Scenario: find_many with contains operator
    When I call users.prisma.find_many(where={email: {"contains": "@test"}})
    Then the SQL should contain "LIKE"

  @precommit
  Scenario: find_many with order_by
    When I call users.prisma.find_many(order_by={created_at: "desc"})
    Then the SQL should contain "ORDER BY"
    And the SQL should contain "DESC"

  @precommit
  Scenario: create operation
    When I call users.prisma.create(data={email: "test@test.com"})
    Then the SQL should contain "INSERT INTO users"
    And the SQL should contain "email"

  @precommit
  Scenario: update operation
    When I call users.prisma.update(where={id: "abc-123"}, data={status: "inactive"})
    Then the SQL should contain "UPDATE users"
    And the SQL should contain "SET"
    And the SQL should contain "WHERE"

  @precommit
  Scenario: delete operation
    When I call users.prisma.delete(where={id: "abc-123"})
    Then the SQL should contain "DELETE FROM users"
    And the SQL should contain "WHERE"
