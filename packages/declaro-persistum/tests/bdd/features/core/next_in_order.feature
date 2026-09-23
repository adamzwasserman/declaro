@bdd @specification
Feature: next_in_order -- The next write, in arrival order
  The crew keeps the order writes arrived in, and a drainer takes them one at a
  time in that order.

  It answers and it does not take. A pure function cannot remove anything, so
  two drainers asking at the same instant are told the same thing and the one
  that takes it is the boundary.

  Scenario: the first handed over is the first taken
    Given three writes handed to the crew in a known order
    Then they come out in that order

  Scenario: a crew holding nothing answers nothing
    Given a crew holding nothing
    Then NOTHING comes back, which is not a failure

  Scenario: asking does not take
    Given three writes handed to the crew in a known order
    Then asking twice answers the same both times, and the crew still holds three
