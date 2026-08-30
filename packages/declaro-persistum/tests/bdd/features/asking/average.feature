@bdd @specification
Feature: average -- The mean of one column
  Over no rows there is no mean, and the answer says so rather than dividing by nothing.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Order declaring orders with columns id, person_id and total
    And a database holding three orders for alice totalling 10, 20 and 30

  Scenario: average
    When a caller asks for the average order total
    Then it is given the number 20

  Scenario: average, narrowed
    When a caller asks for the average of order totals over 15
    Then it is given the number 25

  Scenario: average, over no rows
    When a caller asks for the average of order totals over 1000
    Then it is given a named absence
