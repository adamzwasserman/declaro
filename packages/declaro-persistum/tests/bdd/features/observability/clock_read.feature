@bdd @specification
Feature: clock_read -- Asking the clock what time it is
  A pure function reads no clock, so the reading happens here and the answer is
  handed inward. Shutdown names the elapsed time on every line, and a retry's
  wait is worked out from the time it was asked for.

  Scenario: the time is read at the edge and passed in
    When a shutdown line is built
    Then the elapsed time came from this reader, not from inside the pure function

  Scenario: two callers reading at once get the same answer
    When two retries read the clock in the same instant
    Then they get the same time
    And what keeps them from waiting in step is the ticket, not the clock
