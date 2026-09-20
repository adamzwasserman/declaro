@bdd @specification
Feature: receipt_for -- What the engine did, in what a caller reads
  Built from the outcome of one deposited write, and the one place ok false has a
  meaning: the engine stayed busy for the whole retry budget.

  Scenario: a write that landed
    Given a write that ran on the first attempt
    Then the receipt says it succeeded and carries no failure

  Scenario: a budget that ran out
    Given a write that conflicted past the budget
    Then the receipt says it did not land
    And its failure says what to do differently
