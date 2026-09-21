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

  # ------------------------------------------------------------------
  # FOLDED IN FROM `running/how_many_connections_and_when.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # "The library chooses the connection, opens it, and closes it" says nothing about how
  # many, or what happens when many callers ask at once. A crew answers that for writes
  # on a local database. Nothing answers it for reads, and a read that queues behind a
  # writer is a read that did not have to wait.
