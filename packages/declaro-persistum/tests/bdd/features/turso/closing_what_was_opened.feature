@bdd
Feature: Closing a connection and saying so
  As a caller that has finished with a database
  I want the close to report that it happened
  So that "nothing was open" and "nothing was closed" are different answers

  # TWO FUNCTIONS, ONE BODY: close the thing and return one. What differed was
  # where the thing came from -- one takes it as an argument, the other closes
  # the connection a replicated database holds.
  #
  # THE COUNT IS WHY THEY RETURN ANYTHING. A local file has no held connection
  # and answers zero, so a caller reading the number can tell a release that
  # closed something from one that had nothing to close.

  @precommit
  Scenario: closing a connection reports that one closed
    Given a connection that is open
    When it is closed
    Then one connection was closed
    And the connection is shut

  @precommit
  Scenario: releasing a replica closes the connection it holds
    Given a replicated database holding a connection
    When it is released
    Then one connection was closed
    And the connection is shut

  @precommit
  Scenario: releasing a local database closes nothing, and says so
    Given a database with no primary
    When it is released
    Then no connection was closed
