@bdd @specification
Feature: crew_wait_for_work -- A drainer asleep until there is something to do
  A drainer that keeps looking instead of sleeping starves the caller waiting on
  it: measured before the rewrite, a busy loop hung about one run in six. So it
  sleeps, and three things wake it.

  Scenario: a write arriving wakes it
    Given a drainer waiting on an empty crew
    When a caller deposits a write
    Then it wakes

  Scenario: a write already there does not put it to sleep
    Given a crew already holding a write
    When a drainer waits for work
    Then it comes back without sleeping

  Scenario: the stop wakes it
    Given a drainer waiting on an empty crew
    When the crew is told to stop
    Then it wakes

  Scenario: the idle limit wakes it
    Given a drainer waiting on an empty crew with a short idle limit
    Then it wakes on its own when the limit runs out

  # WHY WAKING ON THE IDLE LIMIT IS NOT A FAILURE. A drainer that woke because
  # nothing happened has had a free turn, and `orchestrator fn lifecycle_catch_up`
  # spends one of those syncing. A fault here would make an idle database look
  # like a broken one on every turn.
  Scenario: waking with nothing to do is an ordinary answer
    Given a drainer waiting on an empty crew with a short idle limit
    Then what comes back when the limit runs out is not a fault

  Scenario: it leaves the bell ready for the next sleeper
    Given a crew already holding a write
    When a drainer waits for work
    And that write is taken and another drainer waits
    Then the second drainer sleeps rather than waking at once
