@bdd @specification
Feature: permitted_connections -- How many connections this database may hold
  The deployment states the number and nothing overrides it. The measurement
  that used to override it, a replica taking one connection, was about the one
  channel to the primary, which no drainer ever touches.

  Scenario: the deployment's number is the answer
    Given a deployment asking for eight
    Then eight are permitted

  Scenario: a database that copies to the cloud is no different
    Given a deployment asking for eight
    Then it does not matter whether the database copies to the cloud

  Scenario: an unset variable is a case, not a default
    Given no number set
    Then the answer says so

  Scenario: a number nobody could open is refused
    Given a deployment asking for none at all
    Then the answer says so, and names what it was asked for
