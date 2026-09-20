@bdd @specification
Feature: lifecycle_register -- Putting a database on the list shutdown works through
  A database that asked to replicate at exit adds itself as it opens. One that
  did not stays off, so a caller who never opted in changes nothing about exit.

  Scenario: a replicating database is on the list
    When a database is opened asking to replicate at exit
    Then shutdown will work through it

  Scenario: a local database is not
    When a database is opened asking only to exit
    Then shutdown has nothing to push for it
