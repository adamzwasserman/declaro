@bdd @specification
Feature: query_run_statement -- Running one statement on the database's connection
  One outward call, for reads and for the DDL a migration runs.
  `docs/architecture.md:156` keeps DDL off the crew: a table created on one MVCC
  connection is invisible to every other.

  Scenario: a read comes back as rows
    When a select is run
    Then rows come back

  Scenario: a schema change runs on the database's own connection
    When a migration runs a create table
    Then it ran on the connection the database holds, not on a drainer's

  Scenario: a refusal comes back as a fault
    When a statement the engine refuses is run
    Then a fault comes back carrying the engine's own error

  # ADDED 2026-09-22. Every function upstream was built so that a caller's value
  # cannot reach a statement's text, and this is the last place that could break
  # it. Nothing said so, so nothing ran it.
  Scenario: a value that reads like the end of a statement is stored as written
    When a value carrying a quote and a DROP is stored and read back
    Then the value comes back character for character, and the table is still there
