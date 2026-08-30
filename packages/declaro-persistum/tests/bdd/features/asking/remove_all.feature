@bdd @specification
Feature: remove_all -- Declare that every matching row no longer exists
  A narrowing is required. Removing everything is a thing a caller can mean, and it has to be said rather than defaulted into.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: remove_all
    When a caller declares that everyone under 18 no longer exists
    Then it is told 1 row changed, and the database holds two people

  Scenario: remove_all, matching nothing
    When a caller declares that everyone over 100 no longer exists
    Then it is told 0 rows changed

  Scenario: remove_all, with no narrowing at all
    When a caller removes every row with no narrowing
    Then it refuses and says a narrowing is required
