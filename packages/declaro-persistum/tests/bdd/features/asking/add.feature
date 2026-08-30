@bdd @specification
Feature: add -- Declare that a row exists
  A list lands together or not at all. That is the only all-or-nothing here, and it is why a group is a list rather than several calls.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: add
    When a caller declares that dave exists
    Then it is told 1 row changed, and the database holds four people

  Scenario: add, several together
    When a caller declares that dave and erin exist together
    Then the database holds five people

  Scenario: add, where the group cannot finish
    When a caller declares dave and a person whose key is already taken
    Then it is refused, and the database still holds three people

  Scenario: add, a key that is already taken
    When a caller declares a person whose key is already taken
    Then it is refused and the engine's own refusal reaches the caller
