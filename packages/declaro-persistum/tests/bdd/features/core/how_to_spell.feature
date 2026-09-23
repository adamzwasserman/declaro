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

  Scenario: Turso rebuilds too
    Given alter_column and turso
    Then the answer says the table is rebuilt

  Scenario: PostgreSQL alters a column in place
    Given alter_column and postgresql
    Then the answer says nothing is rebuilt

  # A REBUILD IS FOUR STATEMENTS AND THE ORDER IS THE WHOLE OF IT. Make the new
  # table, copy what survives, drop the old one, put the new one under the old
  # name. Any other order loses the data.
  Scenario: a rebuild says what it takes, in order
    Given alter_column and sqlite
    Then the answer carries four statements, and the copy comes before the drop

  Scenario: every kind of change answers on every engine
    When all thirteen kinds are asked of all three engines
    Then every pair answers

  Scenario: a kind of change nobody wrote down is a fault, not a guess
    Given a kind of change the table does not cover
    Then a fault comes back naming it and the engine
