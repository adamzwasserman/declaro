@bdd @specification
Feature: spell_operation -- One schema change into the statements that carry it out
  The schema half's compiler. One operation in, and the statements for this
  engine out, which may be several: a SQLite table rebuild is one operation and
  many statements.

  Scenario: an added column is one statement on PostgreSQL
    When adding a column is spelled for postgresql
    Then one statement comes back

  Scenario: the same change is a rebuild on SQLite
    When dropping a column is spelled for sqlite
    Then several statements come back, and they rebuild the table

  Scenario: an operation this engine cannot express is refused
    When an operation no engine table covers is spelled
    Then a fault comes back naming the operation and the engine
