@bdd @specification
Feature: crew_size -- How many drainers this database runs
  The deployment asks, through an environment variable, and one rule overrides
  it. `docs/architecture.md:154`: a replicated database holds one connection by
  construction, so N drainers there are N coroutines queued behind one lock.

  Scenario: a local database runs what was asked for
    Given a deployment asking for eight
    And a local database
    Then eight drainers run

  Scenario: a replicated database runs one
    Given the same deployment asking for eight
    And a replicated database
    Then one drainer runs

  Scenario: an unset variable is a case, not a default
    Given no crew size set
    Then the answer says so rather than picking a number quietly
