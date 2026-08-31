@bdd @specification
Feature: ordered_by -- Every matching row, in a named order
  An order is a mapping of declared columns to "ascending" or "descending", and the keys break ties left to right. Every key is checked against the model, so an undeclared one is refused before a statement exists.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: ordered_by
    When a caller asks for people ordered by name
    Then it is given alice, then bob, then carol

  Scenario: ordered_by, descending
    When a caller asks for people ordered by name descending
    Then it is given carol, then bob, then alice

  Scenario: ordered_by, two columns
    When a caller asks for people ordered by age then name
    Then the second column breaks ties in the first

  Scenario: ordered_by, a column the model does not declare
    When a caller asks for people ordered by "nickname"
    Then the library refuses and names Person

  # "-name" was how you reversed an order until 2026-08-31. It is refused now
  # by the ordinary rule and not by a rule about the minus: `-name` is not a
  # column Person declares, so the lookup that checks every key refuses it
  # without knowing what a minus means.
  Scenario: ordered_by, a column name carrying a directive
    When a caller asks for people ordered by "-name"
    Then the library refuses and names Person
