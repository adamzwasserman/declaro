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
  # I want the connection I handed over to be shut when the call comes back
  # So that a finished database leaves no OS thread running
  #
  # REWRITTEN 2026-09-21. Three scenarios demanded a count back -- one closed,
  # one closed, none closed -- and the declaration's card takes one connection
  # and answers NOTHING or a `Fault`. There is no number for a caller to read,
  # and a scenario that asks for one describes a function nobody declared.
  # Which connection to close is the caller's to decide before it calls, so the
  # three collapse into the one thing a caller can observe: the connection it
  # handed over comes back shut.

  @precommit
  Scenario: the connection handed over comes back shut
    Given a connection that is open
    When it is closed
    Then the connection is shut
    And nothing came back with it

  Scenario: a connection is closed even when the work fails
    Given a read that raises partway
    Then the connection it used was closed
