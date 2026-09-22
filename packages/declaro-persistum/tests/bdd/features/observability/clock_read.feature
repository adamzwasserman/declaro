@bdd @specification
Feature: clock_read -- Asking the clock what time it is
  A pure function reads no clock, so the reading happens here and the answer is
  handed inward. Shutdown names the elapsed time on every line, and a retry's
  wait is worked out from the time it was asked for.

  Scenario: the time is read at the edge and passed in
    When a shutdown line is built
    Then the elapsed time came from this reader, not from inside the pure function

  # DELETED 2026-09-21: "two callers reading at once get the same answer". No
  # clock can be built that fails it, and "the same instant" is not a state a
  # test can put two processes into, so it asserted nothing either way. The
  # claim it was reaching for -- two retries that read one time still wait
  # different amounts, because the ticket is what separates them -- belongs to
  # the function that works out a wait, and it is already a scenario there, in
  # `tests/bdd/features/core/next_wait.feature`. It is not repeated here.
