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
