@bdd @specification
Feature: permitted_connections -- How many connections this database may hold
  The deployment states the number. One measured rule overrides it: a replicated
  database holds one sync connection, measured 2026-08-12 against a real primary,
  where a second answers "database tape error: database is busy".

  Scenario: a replica holds one
    Given a replicated database and a deployment asking for eight
    Then one is permitted

  Scenario: a local database holds what the deployment set
    Given a local database and a deployment asking for eight
    Then eight are permitted

  Scenario: an unset variable is a case, not a default
    Given no number set
    Then the answer says so

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

  Scenario: a replicated database still holds one sync connection
    Given a replicated database
    When many callers read and write at once
    Then exactly one connection to the primary exists
    And the reads did not queue behind it
