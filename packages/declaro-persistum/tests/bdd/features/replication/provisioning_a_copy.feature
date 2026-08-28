@bdd @network
Feature: Making a local copy of a remote database before anyone opens it
  As a service starting on a fresh disk
  I want to ask for a local copy at a path and get one
  So that the first request does not wait for a cold download

  # THESE RUN AGAINST THE REAL TURSO PRIMARY named by TEST_TURSO_URL. They
  # carry @network, which is what makes `-m "not network"` a complete answer.
  #
  # WHY THIS FILE EXISTS. One scenario used to replace the whole pyturso driver
  # with a stand-in and assert that the stand-in had been asked for the path.
  # The stand-in accepted any path. The real driver does not, and the first run
  # against it failed on the exact path shape the old scenario used.

  @network
  Scenario: a copy is made where a folder already exists
    Given a path in a folder that exists, with no copy at it yet
    When a copy is asked for at that path
    Then a copy is there
    And the primary is unchanged

  @network
  Scenario: a copy is made where the folder does not exist yet
    Given a path whose folder does not exist
    When a copy is asked for at that path
    Then a copy is there
    And the primary is unchanged

  @network
  Scenario: opening a replica makes its folder too
    Given a path whose folder does not exist
    When a replica is opened at that path
    Then a copy is there
