@bdd @specification
Feature: one -- Exactly one row, or a named absence
  `one` is for a question with a single answer. Two answers is a mistake in the question, and it says so rather than picking.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: one
    When a caller asks for the one person named "bob"
    Then it is given bob alone

  Scenario: one, where there is none
    When a caller asks for the one person named "nobody"
    Then it is given a named absence

  Scenario: one, where there are several
    When a caller asks for the one person aged over 18
    Then it refuses and says how many matched
    And it does not pick one
