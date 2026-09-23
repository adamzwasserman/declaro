@bdd @specification
Feature: how_to_spell -- How one engine writes one schema change
  Thirteen kinds of change and three engines. What each engine writes lives in a
  table, so the speller reads an answer rather than knowing an engine.

  Scenario: creating a table is one statement on every engine
    Given create_table on any engine
    Then the answer is one statement and says nothing is rebuilt

  Scenario: SQLite alters a column by rebuilding the table
    Given alter_column and sqlite
    Then the answer says the table is rebuilt

  Scenario: PostgreSQL alters a column in place
    Given alter_column and postgresql
    Then the answer says nothing is rebuilt

  Scenario: every kind of change answers on every engine
    When all thirteen kinds are asked of all three engines
    Then every pair answers

  Scenario: a pair nobody wrote down is a fault, not a guess
    Given a kind of change and an engine the table does not cover
    Then a fault comes back naming both
