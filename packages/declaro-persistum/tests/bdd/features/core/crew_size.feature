@bdd @specification
Feature: crew_size -- How many drainers this database runs
  The deployment states the number, through an environment variable, and nothing
  overrides it. Copying to the cloud bounds the one channel to the primary, not
  the connections a drainer writes on, so it says nothing about how many
  drainers may run.

  Scenario: the deployment's number is the answer
    Given a deployment asking for eight
    Then eight drainers run

  Scenario: a database that copies to the cloud is no different
    Given a deployment asking for eight
    Then it does not matter whether the database copies to the cloud

  Scenario: an unset variable is a case, not a default
    Given no crew size set
    Then the answer says so rather than picking a number quietly

  Scenario: a number nobody could run is refused
    Given a deployment asking for none at all
    Then the answer says so, and names what it was asked for
