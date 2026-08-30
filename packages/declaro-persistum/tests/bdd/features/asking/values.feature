@bdd @specification
Feature: values -- Only the named columns, as mappings
  `values` is for when a caller wants data rather than a model. What comes back is mappings, and it says so by not being instances.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: values
    When a caller asks for the id and name of every person
    Then it is given three mappings carrying only id and name

  Scenario: values, not instances
    When a caller asks for the id and name of every person
    Then nothing it is given is an instance of Person

  Scenario: values, a column the model does not declare
    When a caller asks for the "nickname" of every person
    Then the library refuses and names Person
