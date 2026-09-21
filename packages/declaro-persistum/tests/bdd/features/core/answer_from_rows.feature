@bdd @specification
Feature: answer_from_rows -- What a read comes back as
  Driver rows in, and out comes what the caller asked for: instances of the
  model, a number, true or false, or a named absence.

  Scenario: rows become instances of the model
    When a caller asked for every person and two rows came back
    Then two Person instances come back

  Scenario: a number comes back as a number
    When a caller asked how many people there are
    Then an integer comes back, not a row carrying one

  Scenario: whether any exist is true or false
    When a caller asked whether any person exists
    Then true or false comes back, and no rows were read

  Scenario: a missing row is a named absence
    When a caller asked for exactly one person and none matched
    Then NOTHING comes back, which is not None and not an empty list

  # ------------------------------------------------------------------
  # FOLDED IN FROM `running/what_comes_back.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # Asking for rows of a model gives instances of that model. Asking for named columns
  # gives mappings. Asking across two models gives something, and what that something is
  # has to be decided rather than discovered.

  Scenario: rows of one model are instances of it
    When a caller asks for the rows of Person
    Then each answer is an instance of Person

  Scenario: named columns are mappings, not half-built instances
    When a caller asks for the id and name of every Person
    Then each answer is a mapping carrying only those keys
    And none of them is an instance of Person

  Scenario: a partial instance is never handed back
    When a caller asks for only some columns
    Then nothing comes back claiming to be a Person with columns missing

  Scenario: a number is a number
    When a caller asks how many people there are
    Then the answer is an integer and not a row carrying one

  Scenario: rows across two models come back as both
    Given Order references Person
    When a caller asks for orders with the person who placed them
    Then each answer carries an Order and a Person, each whole

  Scenario: a column named the same in both models is not ambiguous
    Given Order and Person both declare a column `id`
    When a caller asks across both
    Then each `id` is reachable through the model it belongs to

  Scenario: a group is not a model instance
    When a caller asks for a count grouped by status
    Then each answer carries the group's columns and its measures
    And it is not an instance of the model that was grouped
