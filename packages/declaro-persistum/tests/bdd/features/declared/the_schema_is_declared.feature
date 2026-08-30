@bdd @specification
Feature: The schema is declared, not migrated
  You change a model. The library reads the live database, compares it to your models,
  works out the difference, and applies it. You write no migration scripts, and you
  never say "add a column".

  Section "The schema is declared, not migrated" of HOW-IT-SHOULD-WORK.md.

  Scenario: a declared table that does not exist yet is created
    Given a model Person declaring the table "people" with columns id and name
    And a database holding no tables
    When the library brings the database to the declared state
    Then the database holds a table "people" with columns id and name

  Scenario: a column added to the model is added to the database
    Given a database already holding "people" with columns id and name
    When the model gains a column email
    And the library brings the database to the declared state
    Then the database holds a column email
    And no migration script was written

  Scenario: a column removed from the model is removed from the database
    Given a database already holding "people" with columns id, name and email
    When the model loses the column email
    And the library brings the database to the declared state
    Then the database no longer holds a column email

  Scenario: a database already in the declared state is left alone
    Given a database already matching the models
    When the library brings the database to the declared state
    Then it applies nothing
    And it reports that nothing needed doing

  Scenario: the models are the desired state, not a list of steps
    Given a model Person declaring the table "people" with columns id and name
    When the library is asked what it would do to an empty database
    Then it names the state it will reach
    And nothing in the declaration is an instruction to add or drop
