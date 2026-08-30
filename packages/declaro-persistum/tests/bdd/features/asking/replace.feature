@bdd @specification
Feature: replace -- Declare that a row exists with these values
  Whatever was there before, this is what is there now. One word for the question a caller otherwise asks twice.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: replace
    When a caller declares that a person with bob's key holds new values
    Then the database holds those values under that key

  Scenario: replace, where the row did not exist
    When a caller declares that a person with an unused key exists
    Then the row is created

  Scenario: replace, reports what it did
    When a caller replaces a row that already existed
    Then the answer says whether it created or replaced
