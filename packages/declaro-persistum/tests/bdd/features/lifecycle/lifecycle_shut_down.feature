@bdd @specification
Feature: lifecycle_shut_down -- Getting every local commit to Turso before the process dies
  The one place blocking is correct. `docs/architecture.md:314`: shutdown blocks
  until Turso has everything, retrying instead of giving up, because on ephemeral
  disk anything unreplicated at exit is gone.

  Scenario: it ignores load and takes the connection
    Given a shutdown in progress with writers still working
    When shutdown replication runs
    Then it takes the connection anyway
    And the reason is that politeness during service is data loss at exit

  Scenario: it keeps trying rather than giving up
    Given Turso refusing the first two attempts
    When shutdown replication runs
    Then it tried again and finished

  Scenario: it says what it is doing while it does it
    Given a shutdown that is taking a long time
    Then every line names the database and the time elapsed

  Scenario: a shutdown cut short says so
    Given a platform grace period shorter than the replication needs
    When the process is killed first
    Then the last line says the data has not reached Turso

  Scenario: a local database has nothing to push
    Given a database with no primary
    When the process is shutting down
    Then nothing was pushed, and the connections were closed
