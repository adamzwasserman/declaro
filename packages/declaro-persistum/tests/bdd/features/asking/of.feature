@bdd @specification
Feature: of -- The rows that belong to another row
  The foreign key already declared the relationship. Naming the join again in every query is the duplication this library exists to remove.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people, and a model Order referencing Person
    And a database holding alice with two orders and bob with one

  Scenario: of
    When a caller asks for the orders of alice
    Then it is given alice's two orders

  Scenario: of, narrowed as well
    When a caller asks for the orders of alice over 15
    Then both the relationship and the narrowing apply

  Scenario: of, where there is no declared relationship
    When a caller asks for the orders of a model Order does not reference
    Then it refuses and says the two are not related

  Scenario: of, works out the join itself
    When a caller asks for the orders of alice
    Then the caller named no join and no key
