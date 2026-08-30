@bdd @specification
Feature: change_all -- Declare new values for every matching row
  It reports how many rows it touched, and zero is an answer a caller can act on.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: change_all
    When a caller declares that everyone over 18 is now named "adult"
    Then it is told 2 rows changed

  Scenario: change_all, matching nothing
    When a caller declares that everyone over 100 is now named "adult"
    Then it is told 0 rows changed
    And nothing in the database changed

  Scenario: change_all, with no narrowing at all
    When a caller changes every row with no narrowing
    Then it refuses and says a narrowing is required
