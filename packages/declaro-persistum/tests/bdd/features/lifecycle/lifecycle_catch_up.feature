@bdd @specification
Feature: lifecycle_catch_up -- What a free turn is spent on
  Two uses of one spare moment, and they are not related to each other: run what
  the crew is holding, and if nobody is waiting to write, push and pull.

  EVERY SCENARIO HERE STATES A RESULT. Rewritten 2026-09-21: sixteen of them
  named functions from the deleted source, which told a reader which call to
  make rather than what has to be true. A result survives whatever the code is
  called; a call site does not.

  # REWRITTEN AGAIN 2026-09-24, in the words the declaration uses. Every
  # scenario said "room", and there are no waiting rooms here: there is a crew,
  # and a caller hands it a write and holds a ticket. Three said "primary" where
  # `Database` carries a channel, and one asked for a refusal to be raised to
  # the caller, which this package never does: a failure comes back in the
  # return, and `fn refusal_reaches_caller` is the card that says how.

  # ------------------------------------------------------ running what waits

  Scenario: the crew's writes run one at a time, in arrival order
    Given three writes deposited in a known order
    When a turn arrives
    Then they ran in that order

  Scenario: a turn with nothing waiting writes nothing
    Given a crew holding nothing
    When a turn arrives
    Then no statement ran

  # ------------------------------------------------- retrying, and giving up

  Scenario: a conflict is retried and a refusal is not
    Given one write that conflicts once and one the engine refuses
    When a turn arrives
    Then the first ran on a second attempt
    And the second was recorded against its caller without a second attempt

  Scenario: a caller never writes a retry loop
    Given a write that conflicts once and then lands
    When a turn arrives
    Then its caller was handed one outcome, and it says the write landed

  Scenario: each attempt waits longer than the one before it
    Given a write that conflicts three times before it lands
    When a turn arrives
    Then each wait was longer than the wait before it

  Scenario: two writes conflicting at the same instant do not wait in step
    Given two writes that conflict in the same instant
    When a turn arrives
    Then their waits differ

  Scenario: a write that never stops conflicting stops being retried
    Given a write that conflicts more often than its budget allows
    When a turn arrives
    Then no further attempt is made
    And its caller is told the budget was spent, and how many attempts it took

  Scenario: a spent budget and a refusal reach the caller differently
    Given one write that only ever conflicted and one the engine refused
    When a turn arrives
    Then each caller can tell which of the two happened to its own write

  # --------------------------------------------- syncing, only when it is free

  # A WRITE PUT ON THE CREW BEFORE THE TURN IS DRAINED BY THE TURN, so what
  # this scenario is about is a caller depositing between the draining and the
  # sync, which is the only moment a writer is waiting when the sync would run.
  Scenario: a writer waiting drops the sync pass
    Given a crew still holding a write and a database that replicates
    When a turn arrives
    Then the sync pass was dropped because a writer was waiting

  Scenario: a database with no channel never syncs
    Given a database with no channel and a crew holding nothing
    When a turn arrives
    Then nothing was sent and nothing was brought down

  Scenario: a free turn on a replicating database syncs
    Given a database that replicates and a crew holding nothing
    When a turn arrives
    Then local commits were sent and the primary's changes came down

  Scenario: what is local goes up before what is remote comes down
    Given a database that replicates and a crew holding nothing
    When a turn arrives
    Then the push ran before the pull

  # A DROPPED PASS IS DROPPED, NOT QUEUED. Ten turns that found a writer
  # waiting leave nothing behind them: the next free turn runs one pass, not
  # ten. Nothing records a pass that did not happen, which is what makes that
  # true rather than something to be arranged.
  Scenario: passes dropped under load do not pile up
    Given ten turns that each found a writer waiting
    When a turn arrives with the crew holding nothing
    Then one pass ran, not ten
