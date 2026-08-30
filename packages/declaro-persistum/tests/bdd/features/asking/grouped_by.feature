@bdd @specification
Feature: grouped_by -- One row per group, carrying its measures
  The group's columns and the numbers over it come back together, because a group without its measures is not an answer anyone asked for.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Order declaring orders with columns id, status and total
    And a database holding two paid orders of 10 and 20, and one unpaid of 5

  Scenario: grouped_by
    When a caller asks for the count of orders grouped by status
    Then it is given one row per status, carrying that status and its count

  Scenario: grouped_by, with a total
    When a caller asks for the total of orders grouped by status
    Then paid carries 30 and unpaid carries 5

  Scenario: grouped_by, two columns
    When a caller asks for a count grouped by status and by day
    Then a group is a distinct pair of the two

  Scenario: grouped_by, a column the model does not declare
    When a caller groups by "nickname"
    Then the library refuses and names Order
