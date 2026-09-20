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
