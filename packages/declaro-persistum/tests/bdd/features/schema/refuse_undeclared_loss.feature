@bdd @specification
Feature: refuse_undeclared_loss -- Nothing is destroyed that nobody declared
  A migration runs at open with nobody watching. A column absent from a model is
  not permission to drop it, because a typo is also a column absent from a model.

  Scenario: an undeclared drop stops the open
    Given a plan that would drop the column "nickname"
    And no model asking to lose it
    Then the migration is refused
    And the fault names the column and what to write to allow it

  Scenario: a declared drop runs
    Given a model naming "nickname" as one it means to lose
    Then the column is dropped

  Scenario: a whole table is the same question
    Given a plan that would drop a table no model declares
    And nothing declaring that the table is to go
    Then the migration is refused

  Scenario: a change that loses nothing is not touched by this
    Given a plan that only adds a column
    Then it runs
