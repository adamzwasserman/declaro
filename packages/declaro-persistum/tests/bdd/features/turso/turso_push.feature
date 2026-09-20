@bdd @specification
Feature: turso_push -- Sending local commits to Turso
  One direction of the one sync connection. `HOW-IT-SHOULD-WORK.md` opens by
  insisting this is not a write to the journal: it carries a log to the cloud,
  and it bounds nothing about how many writers can be in flight.

  Scenario: local commits reach Turso
    Given local commits Turso has not seen
    When a push runs
    Then Turso has them

  Scenario: a push failing is a fault, not a silence
    Given Turso refusing the connection
    When a push runs
    Then a fault comes back saying what to do about it
