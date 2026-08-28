@bdd
Feature: What a failed migration tells the person reading it
  As an operator whose deploy stopped
  I want the message to name the operation, the SQL and what the engine said
  So that I am not left to work out where it went wrong from a stack trace

  # A ROLLBACK THAT ALSO FAILED IS THE WORSE CASE, and it says so: the database
  # may be part-migrated and needs looking at by hand. It is a MigrationError
  # with one more thing to report, which is what makes it a subclass rather
  # than a second error.
  #
  # EVERY FIELD IS REQUIRED AND MAY BE ABSENT. Absence is passed in as NOTHING
  # rather than left off, so a caller who has no SQL to report says so, and one
  # who forgot cannot be mistaken for one who chose.

  @precommit
  Scenario: a failed migration names the operation, the SQL and the cause
    When a migration fails on "create_table" running "CREATE TABLE users" with a disk error
    Then the message names the operation
    And the message names the SQL
    And the message names what the engine said

  @precommit
  Scenario: a failure with nothing to report says only what it knows
    When a migration fails with nothing else to report
    Then the message carries no operation, SQL or cause

  @precommit
  Scenario: a rollback that also failed says the database needs looking at
    When a migration fails and the rollback fails too
    Then the message names the operation
    And it says the rollback also failed
    And it says a person has to look at the database

  @precommit
  Scenario: a rollback that worked is not reported as one that failed
    When a migration fails and the rollback works
    Then it does not say the rollback failed

  @precommit
  Scenario: a rollback failure is still a migration failure
    When a migration fails and the rollback fails too
    Then catching a migration failure catches it
