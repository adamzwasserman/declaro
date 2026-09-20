@bdd @specification
Feature: session_connect -- Everything that has to be true before a handle goes back
  One orchestrator, reached by one door. It works out the engine, opens the
  connection, opens the crew's connections, brings the database up to the models,
  and only then hands back the value every later call reads.

  WHY THE MIGRATION IS HERE AND NOT BEHIND A DOOR OF ITS OWN. Nobody is at a
  deploy to ask for one. A handle handed back while the database is still behind
  its models is a handle whose first question fails on a column the models
  declare.

  Background:
    Given models declaring a table "people" with columns id and name

  Scenario: the handle comes back only after the database matches the models
    Given a database with no tables
    When a caller opens it
    Then the table exists before the handle is returned

  Scenario: a database already matching its models is not migrated
    Given a database whose schema already matches the models
    When a caller opens it
    Then no statement was run against it
    And the handle comes back

  Scenario: the engine is worked out before anything is opened
    When a caller opens "postgresql://localhost/todo"
    Then the engine was decided from the target
    And nothing read it back off the connection

  Scenario: a destructive change nobody declared stops the open
    Given a model that no longer declares the column "nickname"
    And no model asking to drop it
    When a caller opens the database
    Then the open is refused
    And the refusal names the column and what to declare to allow it
    And the column is still there

  Scenario: a cold copy is filled before the handle goes back
    Given a replicated database whose local copy is empty
    When a caller opens it
    Then the copy was filled from Turso before the handle was returned

  Scenario: the handle carries what everything downstream reads
    When a caller opens a local database
    Then the handle carries its engine, its connection, its writers and its room
