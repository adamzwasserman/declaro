@bdd @specification
Feature: exists -- Whether any matching row exists
  It answers true or false and reads no rows, which is the whole reason to ask it rather than counting.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: exists
    When a caller asks whether any person named "bob" exists
    Then it answers true

  Scenario: exists, where none does
    When a caller asks whether any person named "nobody" exists
    Then it answers false

  Scenario: exists, reads no rows
    When a caller asks whether any person exists
    Then no row was read to answer
