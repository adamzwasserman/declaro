@bdd @specification
Feature: permitted_connections -- How many connections this database may hold
  The deployment states the number. One measured rule overrides it: a replicated
  database holds one sync connection, measured 2026-08-12 against a real primary,
  where a second answers "database tape error: database is busy".

  Scenario: a replica holds one
    Given a replicated database and a deployment asking for eight
    Then one is permitted

  Scenario: a local database holds what the deployment set
    Given a local database and a deployment asking for eight
    Then eight are permitted

  Scenario: an unset variable is a case, not a default
    Given no number set
    Then the answer says so
