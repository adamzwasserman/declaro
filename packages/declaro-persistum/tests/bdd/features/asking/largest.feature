@bdd @specification
Feature: largest -- The highest value of one column
  It answers with the value, not with the row that holds it.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Order declaring orders with columns id, person_id and total
    And a database holding three orders for alice totalling 10, 20 and 30

  Scenario: largest
    When a caller asks for the largest order total
    Then it is given the number 30

  Scenario: largest, narrowed
    When a caller asks for the largest order total under 25
    Then it is given the number 20

  Scenario: largest, over no rows
    When a caller asks for the largest order total over 1000
    Then it is given a named absence
