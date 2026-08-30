@bdd @specification
Feature: distinct -- The named columns with duplicates removed
  Duplicate means the whole combination, not one column of it.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and city
    And a database holding two people in Paris and one in Berlin

  Scenario: distinct
    When a caller asks for the distinct cities
    Then it is given Paris and Berlin, once each

  Scenario: distinct, over two columns
    When a caller asks for the distinct pairs of city and name
    Then a pair is a duplicate only when both halves match

  Scenario: distinct, a column the model does not declare
    When a caller asks for the distinct "nickname"
    Then the library refuses and names Person
