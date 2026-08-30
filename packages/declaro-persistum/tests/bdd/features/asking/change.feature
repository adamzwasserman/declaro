@bdd @specification
Feature: change -- Declare new values for one row
  It changes the row that instance names and no other, and it says how many rows it touched.

  One function, one feature. Named in HOW-IT-SHOULD-WORK.md under "What you can
  ask for". The function is declared in `declaro_persistum/asking.py` and raises,
  so every scenario here is red until it is built.

  Background:
    Given a model Person declaring people with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: change
    When a caller declares that bob is now named "robert"
    Then it is told 1 row changed
    And the database holds robert and not bob

  Scenario: change, touching only that row
    When a caller declares that bob is now named "robert"
    Then alice and carol are unchanged

  Scenario: change, a row that is no longer there
    When a caller changes a person the database no longer holds
    Then it is told 0 rows changed

  Scenario: change, a column the model does not declare
    When a caller changes "nickname" on bob
    Then the library refuses and names Person
