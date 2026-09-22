@bdd @specification
Feature: The schema is declared, not migrated
  You change a model. The library reads the live database, compares it to your models,
  works out the difference, and applies it. You write no migration scripts, and you
  never say "add a column".

  Section "The schema is declared, not migrated" of HOW-IT-SHOULD-WORK.md.

  THE DOOR IS `session_open`, AND THERE IS NO OTHER. Written 2026-09-22: these
  scenarios used to say "the library brings the database to the declared state",
  which reads as a call somebody could go looking for. The declaration has one
  caller door, and `orchestrator fn session_connect` behind it is what reaches
  the whole migration. Opening the database is when it happens, and binding
  these scenarios means opening one.

  Scenario: a declared table that does not exist yet is created
    Given a model Person declaring the table "people" with columns id and name
    And a database holding no tables
    When the database is opened
    Then the database holds a table "people" with columns id and name

  Scenario: a column added to the model is added to the database
    Given a database already holding "people" with columns id and name
    When the model gains a column email
    And the database is opened
    Then the database holds a column email
    And no migration script was written

  # A COLUMN MISSING FROM A MODEL IS NOT PERMISSION TO DROP IT, because a typo
  # is also a column missing from a model. Rewritten 2026-09-21: this used to
  # teach drop-on-absence, which would have deleted a column every time somebody
  # misspelled one. Asking to lose it is a declaration like any other, so nobody
  # has to be standing at a deploy to give it.
  Scenario: a column deleted from the model is kept, and the migration stops
    Given a database already holding "people" with columns id, name and email
    When the model loses the column email and nothing asks to drop it
    And the database is opened
    Then the database still holds a column email
    And the refusal names email and says how to ask for it to go

  Scenario: a column the model asks to lose is dropped
    Given a database already holding "people" with columns id, name and email
    When the model loses the column email and declares it dropping
    And the database is opened
    Then the database no longer holds a column email

  Scenario: a table whose model was deleted is kept, and the migration stops
    Given a database already holding "people" and "orders"
    When the model for "orders" is deleted and nothing asks to drop the table
    And the database is opened
    Then the database still holds "orders"
    And the refusal names "orders" and says how to ask for it to go

  Scenario: a database already in the declared state is left alone
    Given a database already matching the models
    When the database is opened
    Then it applies nothing
    And it reports that nothing needed doing

  # THE DATABASE IS BROUGHT TO THE DECLARED STATE WHEN IT IS OPENED, and there is
  # no door that asks what would happen without doing it. Rewritten 2026-09-21:
  # this scenario used to ask the library to describe a plan, which is a door the
  # map does not have. What it was really saying survives without one.
  Scenario: the models are the desired state, not a list of steps
    Given a model Person declaring the table "people" with columns id and name
    When the database is opened
    Then it reaches the state the model names
    And nothing the model carries is an instruction to add or drop
