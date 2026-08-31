@bdd @specification
Feature: What comes back, and in what shape
  Asking for rows of a model gives instances of that model. Asking for named columns
  gives mappings. Asking across two models gives something, and what that something is
  has to be decided rather than discovered.

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
