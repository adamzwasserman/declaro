@bdd @specification
Feature: session_open_writer -- One connection per drainer, held for its life
  A drainer writes and never reads, and it keeps one connection for as long as it
  lives. `docs/architecture.md:148`: every connection is an OS thread, so opening
  one per write pays for that thread every time, and reuse alone is a 6x
  difference.

  Scenario: the crew's connections are opened at open
    Given a deployment asking for a crew of four
    When a local database is opened
    Then four writer connections were opened
    And they were opened before the handle came back

  # A DATABASE THAT COPIES TO THE CLOUD OPENS THE SAME WRITERS AS ANY OTHER.
  # It used to open none, from a measurement that a second connection to a
  # replica is refused. That measurement was about the one channel to the
  # primary, which is the thing there is exactly one of. A drainer writes on a
  # connection to the local file and never touches the channel.
  Scenario: a database that copies to the cloud opens its crew too
    Given a deployment asking for a crew of four and a database that copies to the cloud
    When it is opened
    Then four writer connections were opened

  Scenario: a writer is not the connection reads run on
    When a local database is opened with a crew of one
    Then the writer is a different connection from the one the database reads on

  # ADDED 2026-09-22, and it was not one of the three this file was written
  # with. The three above all reach a database that answers, so every one of
  # them passes whether a refusal comes back in the return or is thrown. A
  # drainer is opened while a process is starting up, and a target that cannot
  # be reached is the commonest thing that happens there.
  Scenario: a database that cannot be reached is a fault, not a handle
    When a writer is asked for against a target that cannot be opened
    Then a fault comes back naming the target
    And no connection is handed back
