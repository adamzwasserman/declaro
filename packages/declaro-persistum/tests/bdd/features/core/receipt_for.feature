@bdd @specification
Feature: receipt_for -- What the engine did, in what a caller reads
  Built from the outcome of one deposited write, and the one place ok false has a
  meaning: the engine stayed busy for the whole retry budget.

  Scenario: a write that landed
    Given a write that ran on the first attempt and changed one row
    Then the receipt says it succeeded and carries no failure
    And it says one row was touched

  Scenario: a budget that ran out
    Given a write that conflicted past the budget
    Then the receipt says it did not land
    And its failure says what to do differently
    And it says no rows were touched

  Scenario: how many times it tried is in the answer
    Given a write that succeeded on the third attempt and changed one row
    Then the answer says it took three

  Scenario: a write that matched nothing is not a failure
    Given a write that ran on the first attempt and changed no rows
    Then the receipt says it succeeded and carries no failure
    And it says no rows were touched
