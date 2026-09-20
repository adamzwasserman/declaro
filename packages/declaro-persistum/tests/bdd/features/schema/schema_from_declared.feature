@bdd @specification
Feature: schema_from_declared -- The target schema, derived from the declarations
  The models are the desired state. Everything else is derived from them, and the
  database is never asked what belongs.

  Scenario: a model becomes a table
    Given a model declaring "people" with id and name
    When the target schema is built
    Then it holds a table "people" with those two columns

  Scenario: a declared view is in the target too
    Given a view declared against that model
    When the target schema is built
    Then the view is in it

  Scenario: nothing in the live database reaches the target
    Given a database holding a table no model declares
    When the target schema is built
    Then the target does not mention it
