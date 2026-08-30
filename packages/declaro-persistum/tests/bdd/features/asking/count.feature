@bdd @specification
Feature: count -- How many rows match
  It returns the number, not a row carrying the number.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: count
    When a caller asks how many people there are
    Then it is given the number 3

  Scenario: count, narrowed
    When a caller asks how many people are over 18
    Then it is given the number 2

  Scenario: count, matching nothing
    When a caller asks how many people are named "nobody"
    Then it is given the number 0

  Scenario: count, is a number and not a row
    When a caller asks how many people there are
    Then the answer is an integer
