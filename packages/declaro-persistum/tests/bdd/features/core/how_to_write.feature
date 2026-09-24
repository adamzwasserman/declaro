@bdd @specification
Feature: how_to_write -- What one engine needs around a write
  `docs/architecture.md:56` says what differs between engines lives in a table
  rather than in branches, with one row per engine. This reads the row for
  writing and hands back what it says. Whoever runs the write reads that answer
  and never learns an engine's name.

  Scenario: Turso opens a write so drainers overlap
    Given turso
    Then the answer opens with a concurrent begin

  Scenario: SQLite takes the write lock up front
    Given sqlite
    Then the answer opens with an immediate begin

  Scenario: PostgreSQL opens nothing and commits nothing
    Given postgresql
    Then the answer opens with no statement at all
    And it says there is no commit to call

  Scenario: every engine answers
    When every engine the package names is asked
    Then each one answers, and no two engines get the same answer

  # ADDED 2026-09-24. The answer carries how values go to the driver, and it said
  # "named" for SQLite and Turso, which write `?`. Nothing read the field, so
  # nothing caught it: every runner in this package hands the values over as a
  # list and never asks. A field nobody reads is a field nobody keeps true.

  Scenario: how values go to the driver is what the compiler actually writes
    When every engine the package names is asked
    Then each one's parameter style matches the placeholders its statements carry
