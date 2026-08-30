@bdd @specification
Feature: remove -- Declare that one row no longer exists
  It removes the row that instance names and no other.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: remove
    When a caller declares that bob no longer exists
    Then it is told 1 row changed, and the database holds two people

  Scenario: remove, touching only that row
    When a caller declares that bob no longer exists
    Then alice and carol are still there

  Scenario: remove, a row that is already gone
    When a caller removes a person the database no longer holds
    Then it is told 0 rows changed
