@bdd @specification
Feature: budget_spent -- Whether the retry budget is gone
  A bounded budget, and running out of it is an answer in its own right,
  distinguishable from the write being refused.

  Scenario: inside the budget
    Given two attempts against a budget of three
    Then there is another attempt to make

  Scenario: the budget is gone
    Given three attempts against a budget of three
    Then it is spent

  Scenario: how many times it tried is in the answer
    Given a write that succeeded on the third attempt
    Then the answer says three
