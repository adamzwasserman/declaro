@bdd @specification
Feature: page -- One page of rows, counting pages from one
  Paging is what callers mean when they reach for limit and offset, and doing it by hand is where the off-by-one lives.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: page
    When a caller asks for page 1 of size 2, ordered by name
    Then it is given alice and bob

  Scenario: page, the last one
    When a caller asks for page 2 of size 2, ordered by name
    Then it is given carol alone

  Scenario: page, past the end
    When a caller asks for page 9 of size 2, ordered by name
    Then it is given no people

  Scenario: page, counted from one
    When a caller asks for page 0 of size 2
    Then it refuses and says pages are counted from one
