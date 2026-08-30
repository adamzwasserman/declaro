@bdd @specification
Feature: count_distinct -- How many different values one column holds
  This is the question `count` cannot answer, which is why it is its own word rather than a flag on `count`.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and city
    And a database holding two people in Paris and one in Berlin

  Scenario: count_distinct
    When a caller asks how many different cities there are
    Then it is given the number 2

  Scenario: count_distinct, narrowed
    When a caller asks how many different cities people named "alice" are in
    Then it counts only the matching rows

  Scenario: count_distinct, over no rows
    When a caller asks how many different cities nobody is in
    Then it is given the number 0
