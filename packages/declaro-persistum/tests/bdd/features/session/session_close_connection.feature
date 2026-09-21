@bdd @specification
Feature: session_close_connection -- What was opened is closed
  `docs/architecture.md:314` ends shutdown with the close, after the push. A crew
  holding one connection per drainer for life means a connection left open is an
  OS thread left running.

  Scenario: every connection opened at open is closed at shutdown
    Given a local database opened with a crew of three
    When the process is shutting down
    Then all four connections are closed, the three writers and the one the database reads on

  Scenario: closing happens after the push, never before
    Given a replicated database with local commits Turso has not seen
    When the process is shutting down
    Then the commits reached Turso before anything was closed

  Scenario: a close that fails is returned, not swallowed
    Given a connection the driver refuses to close
    When shutdown closes it
    Then the fault comes back to the caller

  # ------------------------------------------------------------------
  # FOLDED IN FROM `turso/closing_what_was_opened.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As a caller that has finished with a database
  # I want the close to report that it happened
  # So that "nothing was open" and "nothing was closed" are different answers
  # TWO FUNCTIONS, ONE BODY: close the thing and return one. What differed was
  # where the thing came from -- one takes it as an argument, the other closes
  # the connection a replicated database holds.
  #
  # THE COUNT IS WHY THEY RETURN ANYTHING. A local file has no held connection
  # and answers zero, so a caller reading the number can tell a release that
  # closed something from one that had nothing to close.
  # @precommit

  Scenario: closing a connection reports that one closed
    Given a connection that is open
    When it is closed
    Then one connection was closed
    And the connection is shut

  @precommit
  Scenario: releasing a replica closes the connection it holds
    Given a replicated database holding a connection
    When it is released
    Then one connection was closed
    And the connection is shut

  @precommit
  Scenario: releasing a local database closes nothing, and says so
    Given a database with no primary
    When it is released
    Then no connection was closed

  Scenario: a connection is closed even when the work fails
    Given a read that raises partway
    Then the connection it used was closed
