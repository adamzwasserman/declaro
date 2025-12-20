@bdd
Feature: Django-Style Query API
  As a Django developer
  I want to use familiar QuerySet patterns
  So that I can be productive immediately

  Background:
    Given a users table with Django-style interface
    And the users table has columns: id, email, name, status, age, created_at

  # =============================================================================
  # Pre-commit Tests (Fast, Mocked)
  # =============================================================================

  @precommit
  Scenario: Basic filter with exact match
    When I call users.objects.filter(status="active")
    Then the SQL should contain "WHERE"
    And the SQL should contain "users.status"

  @precommit
  Scenario: Filter with __gt lookup
    When I call users.objects.filter(age__gt=18)
    Then the SQL should contain "WHERE"
    And the SQL should contain ">"

  @precommit
  Scenario: Filter with __gte lookup
    When I call users.objects.filter(age__gte=21)
    Then the SQL should contain "WHERE"
    And the SQL should contain ">="

  @precommit
  Scenario: Filter with __lt lookup
    When I call users.objects.filter(age__lt=65)
    Then the SQL should contain "WHERE"
    And the SQL should contain "<"

  @precommit
  Scenario: Filter with __lte lookup
    When I call users.objects.filter(age__lte=30)
    Then the SQL should contain "WHERE"
    And the SQL should contain "<="

  @precommit
  Scenario: Filter with __contains lookup
    When I call users.objects.filter(email__contains="@test.com")
    Then the SQL should contain "LIKE"

  @precommit
  Scenario: Filter with __isnull lookup
    When I call users.objects.filter(name__isnull=True)
    Then the SQL should contain "IS NULL"

  @precommit
  Scenario: Chained filters
    When I call users.objects.filter(status="active")
    And I chain .filter(age__gte=21)
    Then the SQL should contain "status"
    And the SQL should contain ">="

  @precommit
  Scenario: Exclude filter
    When I call users.objects.exclude(status="inactive")
    Then the SQL should contain "WHERE"
    And the SQL should contain "!="

  @precommit
  Scenario: Ascending order
    When I call users.objects.order("created_at")
    Then the SQL should contain "ORDER BY"

  @precommit
  Scenario: Descending order
    When I call users.objects.order("-created_at")
    Then the SQL should contain "ORDER BY"
    And the SQL should contain "DESC"
