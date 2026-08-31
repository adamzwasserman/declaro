@bdd @specification
Feature: How many connections, and when
  "The library chooses the connection, opens it, and closes it" says nothing about how
  many, or what happens when many callers ask at once. A crew answers that for writes
  on a local database. Nothing answers it for reads, and a read that queues behind a
  writer is a read that did not have to wait.

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
