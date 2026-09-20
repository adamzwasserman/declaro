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
