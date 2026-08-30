@bdd @specification
Feature: sum -- The total of one column
  Over no rows the total is nothing, not zero. Zero is a real total and saying it here would hide an empty question.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Order declaring orders with columns id, person_id and total
    And a database holding three orders for alice totalling 10, 20 and 30

  Scenario: sum
    When a caller asks for the total of order totals
    Then it is given the number 60

  Scenario: sum, narrowed
    When a caller asks for the total of order totals over 15
    Then it is given the number 50

  Scenario: sum, over no rows
    When a caller asks for the total of order totals over 1000
    Then it is given a named absence, not the number 0

  Scenario: sum, over a column that holds no numbers
    When a caller asks for the total of a text column
    Then it refuses and names the column
