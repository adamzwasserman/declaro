@bdd @specification
Feature: lifecycle_catch_up -- What a free turn is spent on
  Two uses of one spare moment, and they are not related to each other: empty the
  room, and if nothing else wants the connection, push and pull.

  EVERY SCENARIO HERE STATES A RESULT. Rewritten 2026-09-21: sixteen of them
  named functions from the deleted source, which told a reader which call to
  make rather than what has to be true. A result survives whatever the code is
  called; a call site does not, and this package has just deleted 30,000 lines
  of call sites.

  # ------------------------------------------------------- draining the room

  Scenario: the room drains one write at a time, in arrival order
    Given three writes deposited in a known order
    When a turn arrives
    Then they ran in that order

  Scenario: a turn with an empty room writes nothing
    Given a room with nothing in it
    When a turn arrives
    Then no statement ran

  # ------------------------------------------------- retrying, and giving up

  Scenario: a conflict is retried and a refusal is not
    Given one write that conflicts once and one the engine refuses
    When a turn arrives
    Then the first ran on a second attempt
    And the second was recorded against its caller without a second attempt

  Scenario: a caller never writes a retry loop
    When a caller declares a write that conflicts once and then succeeds
    Then the caller wrote no loop and saw no conflict

  Scenario: each attempt waits longer than the one before it
    Given a write that conflicts three times before it lands
    When a turn drains it
    Then each wait was longer than the wait before it

  Scenario: two writes conflicting at the same instant do not wait in step
    Given two writes that conflict in the same instant
    When a turn drains them
    Then their waits differ

  Scenario: a write that never stops conflicting stops being retried
    Given a write that conflicts more often than its budget allows
    When a turn drains it
    Then no further attempt is made
    And its caller is told the budget was spent, and how many attempts it took

  Scenario: a spent budget and a refusal reach the caller differently
    Given one write that only ever conflicted and one the engine refused
    When a turn drains both
    Then the spent one comes back as an answer its caller can read
    And the refused one is raised to the caller that wrote it

  # --------------------------------------------- syncing, only when it is free

  Scenario: a writer waiting drops the sync pass
    Given a writer waiting for the connection
    When a turn arrives
    Then no sync pass ran

  Scenario: a dropped pass is dropped, not queued
    Given ten passes dropped under load
    When the load clears
    Then one pass runs, not ten

  Scenario: a database with no primary never syncs
    Given a database with no primary
    When a turn arrives
    Then nothing was sent and nothing was received

  Scenario: a closed database is not synced
    Given a database with a primary that has been closed
    When a turn arrives
    Then nothing was sent and nothing was received

  Scenario: a free turn on a replicated database syncs
    Given a database with a primary and nobody waiting to write
    When a turn arrives
    Then local commits were sent and the primary's changes came down

  Scenario: what is local goes up before what is remote comes down
    Given a database with a primary and local commits it has not seen
    When a turn arrives
    Then the local commits went up first
    And a write waiting since the last shutdown was not overwritten by what came after it

  Scenario: a database with nothing pending takes no connection
    Given a database with a primary and nothing written since the last pass
    When a turn arrives
    Then no connection was taken and no round trip was made

  Scenario: a commit is what makes the next turn worth taking
    Given a database with a primary and nothing pending
    When a commit lands and a turn arrives
    Then a pass ran
