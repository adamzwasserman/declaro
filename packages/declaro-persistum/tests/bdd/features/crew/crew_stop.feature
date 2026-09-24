@bdd @specification
Feature: crew_stop -- Every drainer told to stop, and waited for
  Shutdown has a deadline the platform sets, and a drainer still writing when the
  process dies takes its write with it. So the stop is rung once and then every
  drainer is waited for, rather than asked and left.

  Scenario: every drainer is told
    Given a crew of four drainers running
    When the crew is stopped
    Then every drainer has finished

  Scenario: it waits rather than asking and leaving
    Given a crew of one drainer that takes a moment to notice
    When the crew is stopped
    Then the call did not come back until that drainer had finished

  Scenario: a drainer that raised on its way out is still reported
    Given a crew of two drainers, one of which fails as it stops
    When the crew is stopped
    Then a fault comes back saying one drainer did not stop cleanly
    And the other drainer still finished

  # STOPPING A CREW THAT NEVER STARTED IS AN ORDINARY SHUTDOWN, not a failure. A
  # process that opened a database and wrote nothing still shuts down, and a
  # refusal here would make that look like something went wrong.
  Scenario: a crew with nobody in it stops without complaint
    Given a crew nobody started
    When the crew is stopped
    Then nothing comes back, and it is not a fault

  Scenario: the crew is left holding nobody
    Given a crew of four drainers running
    When the crew is stopped
    Then the crew holds no drainers
