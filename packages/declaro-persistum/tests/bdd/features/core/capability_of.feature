@bdd @specification
Feature: capability_of -- What an engine does natively, and what builds it otherwise
  One table says which capability is native on which engine and what builds it
  where it is not. Split across a function per operation and "native or built"
  ends up encoded in function names, where nobody can read it.

  Scenario: PostgreSQL has a materialized view
    Given postgresql and a materialized view
    Then it is native

  Scenario: SQLite builds one
    Given sqlite and a materialized view
    Then it is built from a table, its triggers and a registry row

  Scenario: a capability nobody wrote down is a fault, not a guess
    Given an engine and a feature the table does not cover
    Then a fault comes back naming both

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/one_table_of_capabilities.feature` on 2026-09-21,
  # one feature per function. What that file said about why:
  # Engines do not offer the same things. PostgreSQL has a materialized view and SQLite
  # does not, so one is built from a table, its triggers and a registry row. One table
  # should say which capability is native on which engine and what builds it where it is
  # not.
  # REWRITTEN AS RESULTS on 2026-09-21. Five scenarios read the shape of a lookup
  # table -- rows, columns, cells, a column added -- rather than asking anything.
  # A caller sees an answer per engine and feature, so that is what the one
  # scenario below asks for, and the four that described the grid are gone.

  Scenario: every engine answers about every feature
    When every engine is asked about every feature the package names
    Then each pair answers, and every answer says the engine has it or names what supplies it

  Scenario: an engine said to have a capability has it
    Given every answer saying an engine has a feature natively
    When that capability is used against that engine
    Then the engine does it

  Scenario: what is built can be created, dropped and read back as itself
    Given a feature an engine builds rather than has
    When it is created on that engine and then read back
    Then it reads back as that feature rather than as the parts it is made of
    And dropping it leaves none of the parts behind

  # ------------------------------------------------------------------
  # FOLDED IN FROM `pragma_support.feature` on 2026-09-21, one feature per
  # function. Four of its five scenarios asked SQLite whether SQLite answers its
  # own PRAGMAs, which measures the engine and not this package, and they were
  # deleted on 2026-09-21. What they were reaching for -- a table's columns, its
  # indexes and its foreign keys read back as the engine really holds them --
  # is one scenario in `schema/schema_inspect.feature` instead.
  # The one below survives because it is about this package: an engine that
  # cannot describe what it holds has to say so where the question was asked.

  Scenario: an engine that cannot be asked what it holds fails loudly
    Given an engine that cannot be asked what a table holds
    When it is asked
    Then the failure reaches the caller, and no empty answer comes back
