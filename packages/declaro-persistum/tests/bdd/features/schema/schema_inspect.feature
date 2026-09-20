@bdd @specification
Feature: schema_inspect -- Asking a database what it currently holds
  The only place this library asks a database what is there, and it asks so it
  can compare, never so it can decide what belongs. What belongs is in the
  models.

  Scenario: the live schema comes back
    Given a database holding one table with two columns
    When it is inspected
    Then the table and both columns come back

  Scenario: an empty database is an answer
    Given a database with no tables
    When it is inspected
    Then an empty schema comes back, and that is not a failure

  Scenario: nothing decides anything from it
    When a database holds a table no model declares
    Then the table is not adopted into the target schema
