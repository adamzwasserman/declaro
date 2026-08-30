@bdd @specification
Feature: having -- Groups narrowed by their measures
  `where` narrows rows and this narrows groups. Asking for one when you meant the other is the mistake this vocabulary should make hard to write.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Order declaring orders with columns id, status and total
    And a database holding two paid orders of 10 and 20, and one unpaid of 5

  Scenario: having
    When a caller asks for statuses whose count is over 1
    Then it is given paid alone

  Scenario: having, narrows groups and not rows
    When a caller asks for statuses whose total is over 25
    Then unpaid is absent because its group total is 5, not because any row was excluded

  Scenario: having, with a row narrowing as well
    When a caller narrows rows to totals over 5 and groups whose count is over 1
    Then both narrowings apply, in that order

  Scenario: having, naming a measure that was not asked for
    When a caller narrows groups by a measure the question does not carry
    Then it refuses and lists the measures the question does carry
