@bdd @specification
Feature: answer_from_receipt -- What a write gives back to the caller
  Every write says how many rows it touched, and a change that matched nothing
  returns zero, which is an answer you can act on. This is the write half of
  what shaping rows is for a read.

  A caller never sees a ticket. The ticket is how the crew joined the write to
  its outcome, and it stops here.

  Scenario: a write that landed gives back its count
    Given a write that changed three rows
    Then the caller is given three

  Scenario: a write that matched nothing gives back zero
    Given a write that changed no rows
    Then the caller is given zero, and it is not a failure

  Scenario: a spent budget reaches the caller as the fault it is
    Given a write whose retry budget ran out
    Then the caller is given that fault, unchanged

  Scenario: no answer carries a ticket
    When every kind of receipt is turned into an answer
    Then not one of them hands the caller a ticket
