@bdd @specification
Feature: session_open_writer -- One connection per drainer, held for its life
  A drainer writes and never reads, and it keeps one connection for as long as it
  lives. `docs/architecture.md:148`: every connection is an OS thread, so opening
  one per write pays for that thread every time, and reuse alone is a 6x
  difference.

  Scenario: the crew's connections are opened at open
    Given a deployment asking for a crew of four
    When a local database is opened
    Then four writer connections were opened
    And they were opened before the handle came back

  Scenario: a replicated database opens no crew
    Given a replicated database
    When it is opened
    Then no writer connection was opened
    And the reason is that a replica holds one connection by construction

  Scenario: a writer is not the connection reads run on
    When a local database is opened with a crew of one
    Then the writer is a different connection from the one the database reads on
