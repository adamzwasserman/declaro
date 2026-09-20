@bdd @specification
Feature: turso_pull -- Bringing down what this copy does not have
  The program asks; nothing arrives on its own, which is what makes this a reader
  rather than a door. `docs/architecture.md:310`: a cold open blocks on it,
  because a database with no schema is unusable rather than merely stale.

  Scenario: a cold open copies the whole database
    Given a local copy with no schema
    When the database is opened
    Then the whole database was copied before the handle came back

  Scenario: a warm open does not block
    Given a local copy that already has the schema
    When the database is opened
    Then the handle came back without waiting for Turso
