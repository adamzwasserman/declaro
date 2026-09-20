@bdd @specification
Feature: query_collect -- Coming back for a write you deposited
  A ticket belongs to exactly one caller, so a failure reaches that caller and
  nobody else, and a caller draws its own boundary by choosing when to collect.

  Scenario: a collected receipt says the write landed
    When a caller deposits a write, the crew drains it, and the caller collects
    Then the receipt says it succeeded
    And the row is in the database

  Scenario: an outright refusal is raised, not put in a receipt
    When a caller deposits a write that violates the primary key and collects it
    Then the engine's own error is raised

  Scenario: a spent retry budget is a receipt, not a raise
    When a write conflicts more often than the budget allows
    Then the receipt says it did not land
    And it says what to do differently

  Scenario: one caller's refusal does not reach another
    When one caller deposits a doomed write and another a good one
    Then the doomed ticket raises and the good one reports success

  Scenario: collecting drops the ticket
    When a caller collects its ticket
    Then the room holds nothing for it any more
