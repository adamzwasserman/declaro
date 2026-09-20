@bdd @specification
Feature: migration_report -- What the migration at open did
  Nobody is watching a deploy, so this is where the record of one lives: what was
  going to run, and what ran.

  Scenario: it carries the plan and the count
    Given a plan of three operations, all applied
    Then the report says three, against that plan

  Scenario: a migration that did nothing says so
    Given a database already matching its models
    Then the report is empty, and that is distinguishable from a failure
