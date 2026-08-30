@bdd @specification
Feature: all -- Every matching row, as instances of the model
  `all` answers with the rows themselves. It is the plainest question, and the one every other row question is a narrowing of.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: all
    When a caller asks for all people
    Then it is given three instances of Person

  Scenario: all, narrowed by a declared column
    When a caller asks for all people named "bob"
    Then it is given one instance, and that person is bob

  Scenario: all, matching nothing
    When a caller asks for all people named "nobody"
    Then it is given no people
    And that is an empty answer, not a named absence

  Scenario: all, narrowed by a column the model does not declare
    When a caller asks for all people narrowed by "nickname"
    Then the library refuses and names Person
