@bdd @specification
Feature: migration_needed -- Whether the database is still behind its models
  A pure comparison of two schemas, run at open, before a handle is handed back.

  Scenario: a database matching its models needs nothing
    Given a live schema equal to the target
    Then no migration is needed

  Scenario: a new column means a migration
    Given a model that has gained a column since the database was brought up
    Then a migration is needed

  Scenario: it opens nothing and asks nobody
    When the comparison runs
    Then it read no clock, no connection and no file
