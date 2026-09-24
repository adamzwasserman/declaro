@bdd @specification
Feature: crew_start -- One drainer per writer, each running the turn it was given
  A drainer holds one connection for its life, so there are exactly as many of
  them as there are writer connections. What each one does on a turn is handed
  in, because a door starts what it is given and never decides what that is.

  Scenario: there is one drainer for every writer
    Given a database opened with four writers
    When the crew is started
    Then four drainers are running
    And the crew is holding all four

  Scenario: each drainer is given a writer of its own
    Given a database opened with four writers
    When the crew is started
    Then no two drainers were given the same connection

  Scenario: a drainer runs the turn it was handed
    Given a database opened with one writer
    When the crew is started
    Then the turn runs

  Scenario: a database with no writers starts nobody
    Given a database opened with no writers
    When the crew is started
    Then no drainer is running
    And it comes back a crew rather than a fault

  # STARTING TWICE IS THE SHAPE THAT LEAKS. The second call would hand the
  # runtime another drainer per writer, and the first set would be running on
  # the same connections with nothing holding them, so `fn crew_stop` would stop
  # half of them and the rest would keep writing after shutdown said they had
  # stopped.
  Scenario: a crew that is already running is not started again
    Given a database opened with one writer
    And a crew that has already been started
    When the crew is started
    Then it is refused, saying the crew is already running
    And no second drainer was started
