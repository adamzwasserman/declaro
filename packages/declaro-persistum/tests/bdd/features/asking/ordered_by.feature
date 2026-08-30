@bdd @specification
Feature: ordered_by -- Every matching row, in a named order
  A leading minus reverses. The order names declared columns, so an undeclared one is refused before a statement exists.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: ordered_by
    When a caller asks for people ordered by name
    Then it is given alice, then bob, then carol

  Scenario: ordered_by, reversed
    When a caller asks for people ordered by "-name"
    Then it is given carol, then bob, then alice

  Scenario: ordered_by, two columns
    When a caller asks for people ordered by age then name
    Then the second column breaks ties in the first

  Scenario: ordered_by, a column the model does not declare
    When a caller asks for people ordered by "nickname"
    Then the library refuses and names Person
