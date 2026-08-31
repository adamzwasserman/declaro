@bdd @specification
Feature: A rename is asked once and remembered
  A column gone and another appeared is either a rename or a drop and an add, and the
  difference is your data. The library says it cannot tell rather than choosing,
  because choosing wrong destroys a column.

  THAT MEANS SOMEBODY HAS TO ANSWER, and the answer has to outlive the run. Asking
  again on every deploy is asking in the middle of a deploy, which is the worst
  possible moment and the one where people say yes to anything.

  Scenario: an ambiguous change stops rather than guessing
    Given a model where `email` is gone and `email_address` appeared
    When the library is asked to bring the database up to date
    Then it stops and says it cannot tell a rename from a drop and an add

  Scenario: the answer is recorded beside the models
    Given an ambiguity that has been answered
    Then the answer is written down where the models live
    And it is a declaration like any other

  Scenario: a remembered answer is not asked again
    Given a recorded answer for this ambiguity
    When the library is asked to bring the database up to date
    Then it applies the recorded decision without asking

  Scenario: the recorded answer names both sides
    When a recorded rename decision is read
    Then it names the column that went and the column that came
    And a decision naming only one is refused

  Scenario: a decision that no longer matches anything is reported
    Given a recorded decision for columns that are both now gone
    When the library is asked to bring the database up to date
    Then it says the decision is stale rather than ignoring it

  Scenario: nothing is applied while an ambiguity is unanswered
    Given three operations, one of them ambiguous
    When the library is asked to bring the database up to date
    Then none of the three is applied
