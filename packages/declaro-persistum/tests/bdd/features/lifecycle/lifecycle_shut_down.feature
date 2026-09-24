@bdd @specification
Feature: lifecycle_shut_down -- Every local commit up the channel before the process dies
  The one place blocking is correct. `docs/architecture.md:314`: shutdown blocks
  until the primary has everything, retrying instead of giving up, because on
  ephemeral disk anything that did not leave is gone.

  # REWRITTEN 2026-09-24, to what this card can answer. It takes the databases
  # that registered, the instant it began and how many pushes it has made, and
  # it answers with those or with a fault. Eight scenarios went to the cards
  # that own them:
  #
  # What one pass leaves behind, and that a pull does not overwrite a local
  # commit -> `channel_push.feature` and `channel_pull.feature`.
  #
  # Whether a pass yields to a waiting writer, and that readers never hold one
  # off -> `lifecycle_catch_up.feature`. That is the opportunistic half, and
  # this one is the opposite: shutdown takes the channel whatever is waiting.
  #
  # Three said "primary" where `Database` carries a channel, and one asked for a
  # database with no primary to be refused, which it is not: a local database
  # has nothing to push and shuts down cleanly.

  Scenario: the crew stops before anything is pushed
    Given a database that replicates, with its crew running
    When the process is shutting down
    Then every drainer had finished before the push ran

  Scenario: it takes the channel whatever is waiting
    Given a database that replicates, with a write still waiting
    When the process is shutting down
    Then the push ran anyway, because politeness at exit is data loss

  Scenario: it keeps trying rather than giving up
    Given a channel refusing the first two attempts
    When the process is shutting down
    Then it tried again and the commits went up

  Scenario: it says what it is doing while it does it
    Given a database that replicates
    When the process is shutting down
    Then every line names the database and how long it has taken

  Scenario: a local database has nothing to push
    Given a database with no channel
    When the process is shutting down
    Then nothing was pushed, and its connections were closed

  Scenario: what was opened is closed, on every database
    Given two databases, one that replicates and one that does not
    When the process is shutting down
    Then every connection either of them held is shut

  # A CHANNEL THAT NEVER TAKES IT IS A SHUTDOWN THAT HAS TO END ANYWAY. The
  # platform kills the process on its own schedule, so there is a point at which
  # trying again is time spent not closing anything. What the caller gets is a
  # fault naming the database, so an operator reading the shutdown knows which
  # one's commits are still on the machine.
  Scenario: a channel that never takes it is reported, not waited on forever
    Given a channel that refuses every attempt
    When the process is shutting down
    Then a fault comes back naming the database, and its connections were still closed
