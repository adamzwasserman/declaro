@bdd @specification
Feature: first -- The first row in a stated order
  The order is required. "First" with no order is whichever row the engine reached, which is not an answer a caller can rely on.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: first
    When a caller asks for the first person ordered by name
    Then it is given alice

  Scenario: first, descending
    When a caller asks for the first person ordered by name descending
    Then it is given carol

  Scenario: first, where nothing matches
    When a caller asks for the first person named "nobody" ordered by name
    Then it is given a named absence

  Scenario: first, with no order given
    When a caller asks for the first person and names no order
    Then it refuses and says an order is required
