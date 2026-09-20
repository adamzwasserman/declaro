@bdd @specification
Feature: next_in_order -- The next write, in arrival order
  The room keeps the order writes arrived in, and a drainer takes them one at a
  time in that order.

  Scenario: the first deposited is the first taken
    Given three writes deposited in a known order
    Then they come out in that order

  Scenario: an empty room answers nothing
    Given an empty room
    Then NOTHING comes back, which is not a failure
