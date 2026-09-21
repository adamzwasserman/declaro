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

  # ------------------------------------------------------------------
  # FOLDED IN FROM `applier/what_an_applier_says_about_itself.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As the migration runner, deciding how to handle a failed operation
  # I want the applier to say which engine it is and how it scopes a batch
  # So that one bad operation on Turso does not roll back the thirty before it
  # THE TRANSACTION MODE IS THE PART THAT MATTERS. SQLite and PostgreSQL wrap
  # the whole batch, so one failure undoes all of it. Turso commits per
  # operation, so a failure keeps what already landed. Reading the wrong one
  # for an engine either loses work or reports work that was undone.
  #
  # Six functions across three modules, each returning a constant. The engine
  # and the mode were the only things that differed.
  # @precommit

  Scenario Outline: each applier names its own engine
    When the "<engine>" applier is asked which engine it is
    Then it says "<engine>"

    Examples:
      | engine     |
      | sqlite     |
      | turso      |
      | postgresql |

  @precommit
  Scenario Outline: each applier says how it scopes a batch
    When the "<engine>" applier is asked how it scopes a batch
    Then it says "<mode>"

    Examples:
      | engine     | mode            |
      | sqlite     | all_or_nothing  |
      | turso      | per_operation   |
      | postgresql | all_or_nothing  |

  @precommit
  Scenario: every applier answers both questions
    When the appliers are listed
    Then each one says which engine it is and how it scopes a batch

  @precommit
  Scenario: the mode is one of the two the runner knows
    When the appliers are listed
    Then no applier names a mode the runner cannot act on

  # WHAT THE RESULT SAYS WHEN NOTHING WENT WRONG. `ApplyResult["error"]` is
  # declared `str | None`, and None is that field's own vocabulary for "no
  # error" rather than a function forgetting to answer. These two scenarios
  # make it a tested answer: the checker cannot tell a declared None from an
  # unwritten one, and only a caller reading the field can.

  @precommit
  Scenario: a batch with nothing skipped reports no error
    When the skip summary is asked about an empty list
    Then the error field is empty

  @precommit
  Scenario: a batch with skips names every one of them
    When the skip summary is asked about two skipped operations
    Then the error field counts them and names both

  # THE RENDERING TABLES. Each engine has one, and PostgreSQL's is built from
  # SQLite's with the rows that differ replaced. That reuse is the thing worth
  # holding: an operation added to SQLite's table and forgotten for PostgreSQL
  # would be an operation the runner cannot render on that engine.

  @precommit
  Scenario: every engine can render every operation the differ emits
    When the rendering tables are compared with the operations the differ emits
    Then each engine can render every one of them

  @precommit
  Scenario: PostgreSQL alters a column in place rather than rebuilding
    When the PostgreSQL rendering table is read
    Then it has no whole-table rebuild for a column change

  # ------------------------------------------------------------------
  # FOLDED IN FROM `exceptions/what_a_failed_migration_says.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As an operator whose deploy stopped
  # I want the message to name the operation, the SQL and what the engine said
  # So that I am not left to work out where it went wrong from a stack trace
  # A ROLLBACK THAT ALSO FAILED IS THE WORSE CASE, and it says so: the database
  # may be part-migrated and needs looking at by hand. It is a MigrationError
  # with one more thing to report, which is what makes it a subclass rather
  # than a second error.
  #
  # EVERY FIELD IS REQUIRED AND MAY BE ABSENT. Absence is passed in as NOTHING
  # rather than left off, so a caller who has no SQL to report says so, and one
  # who forgot cannot be mistaken for one who chose.
  # @precommit

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
