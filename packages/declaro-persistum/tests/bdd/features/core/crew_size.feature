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

  Scenario: a read does not wait for a writer
    Given a write in flight
    When a caller reads
    Then the read answers without waiting for the write to finish

  Scenario: reads run beside each other
    When five callers read at once, each taking time
    Then the five overlap rather than running one after another

  Scenario: a caller never sees a connection whatever happens
    When many callers read and write at once
    Then no connection reached any caller

  Scenario: the number of connections is bounded
    When a thousand callers ask at once
    Then the number of connections opened stays under a stated bound
    And the bound is a declared number, not whatever the machine allowed

  Scenario: the bound is stated where the database is opened
    When a database is opened
    Then how many connections it may use is part of that declaration
    And it has no default, because the right number is a property of the machine

  Scenario: a connection is closed even when the work fails
    Given a read that raises partway
    Then the connection it used was closed

  Scenario: a replicated database still holds one sync connection
    Given a replicated database
    When many callers read and write at once
    Then exactly one connection to the primary exists
    And the reads did not queue behind it
