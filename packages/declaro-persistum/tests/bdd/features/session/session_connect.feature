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
    Then no statement changed it
    And the handle comes back

  # MOVED HERE 2026-09-21 from the feature for the crew's own maker. `crew_make`
  # takes no arguments, opens nothing and never sees a database, so it cannot
  # answer what a caller gets back. Building the handle is this orchestrator's
  # work.
  #
  # REWORDED 2026-09-24. It said "waiting room", and there is no waiting room in
  # the declaration. `type Crew` is what holds the writes that have arrived and
  # the drainers that run them, and a scenario naming a thing the declaration
  # does not have is a scenario nothing can bind.
  Scenario: the library makes the crew, not the caller
    When a caller opens it
    Then the crew came with the handle
    And nobody had to make one

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
    Then the copy was filled from the primary before the handle was returned

  # ADDED 2026-10-10, AND THIS IS multicardz'S PRODUCTION FAILURE WRITTEN DOWN. A
  # deploy created a table on the local copy. The push that would have sent it
  # never landed, and the deploy stamped the schema as done anyway. Every open
  # after that pulled, found a copy that matched the models, planned no
  # migration, and sent nothing. The primary went without the table for weeks
  # while every open reported success.
  #
  # MEASURED THE SAME DAY, because the order only matters if a pull keeps what is
  # local: it does. A replica holding an unpushed CREATE TABLE still holds it
  # after a pull, and a push after that pull reaches the primary. So the push
  # goes up before the live schema is read, and a copy whose changes never left
  # is never read as a copy that matches its models.
  Scenario: the push goes up before anything comes down
    When the channel's three steps are read in the order they run
    Then the push runs after the channel opens and before the pull
    And both run before the live schema is read

  # NOBODY TELLS THIS LIBRARY WHICH KIND OF START IT IS, so the open asks the
  # disk. A rebuild wipes the files, a soft restart keeps them, and a provisioned
  # disk keeps them, and the same open has to handle all three. The push above
  # fills a machine that started with nothing, so the only copy that can be
  # behind the primary is one that was already here, and only that one pulls.
  #
  # THE DISK IS ASKED BEFORE ANYTHING OPENS, because opening makes the file:
  # measured 2026-10-10, `turso.aio.connect` leaves a database at a path that
  # held nothing a moment earlier.
  Scenario Outline: the pull runs only when a copy was already on this machine
    Given a replicated open whose copy was <state>
    When the step that brings the primary down runs
    Then the primary was <reached>

    Examples:
      | state        | reached    |
      | already here | asked      |
      | not here yet | left alone |

  Scenario: an open sends what is local to the primary before it reads the live schema
    Given a replicated database holding a table the primary never got
    When a caller opens it
    Then the primary has that table

  Scenario: an open whose push did not land is refused
    Given a replicated database whose push will not land
    When a caller opens it
    Then the open is refused
    And the refusal says the local commits could not be sent

  # REWORDED 2026-09-24. It named Turso, and no scenario here names an engine:
  # a caller writes a target and the library reads the engine off it. It also
  # said "its connection" and "its room". `type Database` carries a list of
  # connections, a list of writers and a crew.
  Scenario: the handle carries what everything downstream reads
    When a caller opens a local database
    Then the handle carries its engine, its connections, its writers and its crew

  # ------------------------------------------------------------------
  # FOLDED IN FROM `applier/what_an_applier_says_about_itself.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As the migration runner, deciding how to handle a failed operation
  # I want one bad operation on Turso not to undo the thirty before it
  # So that a stopped deploy is repaired from what the database actually holds
  #
  # All three engines wrap the whole batch, so one failure undoes all of it.
  # What an operator finds the next morning is stated as what survives rather
  # than as the word an engine answers with.
  #
  # THE TURSO ROW SAID "kept" UNTIL 2026-10-10, on the reasoning that Turso
  # commits each change as it makes it. Measured against pyturso that day and it
  # is false: plain `BEGIN` rolls back a created table and an added column, and a
  # three-statement batch whose middle statement fails leaves the database
  # holding only what it started with.
  #
  # REWRITTEN 2026-09-21. Six scenarios read the migration machinery itself:
  # which engine each piece of it names, which words it uses for the scope of a
  # batch, and whether each engine's table of spellings covers every operation.
  # Replace those tables with anything else and all six break, although no
  # caller sees a change. What a caller does see is below.

  @precommit
  Scenario Outline: what a failed migration leaves behind depends on the engine
    Given models whose migration is thirty operations on "<engine>"
    And the engine refuses the thirtieth
    When a caller opens the database
    Then the twenty-nine before it are "<survives>"
    And a fault comes back instead of a handle

    Examples:
      | engine     | survives |
      | sqlite     | undone   |
      | turso      | undone   |
      | postgresql | undone   |

  @precommit
  Scenario Outline: no change a model declares is refused for want of a spelling
    Given models declaring every kind of change a migration can plan
    When a caller opens a "<engine>" database
    Then every change reached the database
    And none came back refused as one this engine cannot write

    Examples:
      | engine     |
      | sqlite     |
      | turso      |
      | postgresql |

  # WHAT THE HANDLE CARRIES ABOUT THE MIGRATION IT JUST RAN. The declaration's
  # `Report` is the plan and a count of what ran, and nothing else. It has no
  # error string, so the two scenarios that read one are gone. How much of the
  # plan ran is what a caller can still tell, and it is stated on the report the
  # declaration actually has.

  @precommit
  Scenario: the report says how much of the plan actually ran
    Given a plan of five operations, two of which the database already has
    When a caller opens it
    Then the report names all five operations it planned
    And the report says three of them ran

  # ------------------------------------------------------------------
  # FOLDED IN FROM `exceptions/what_a_failed_migration_says.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As an operator whose deploy stopped
  # I want to be told which operation stopped it and what the engine said
  # So that I am not left to work out where it went wrong from a stack trace
  #
  # REWRITTEN 2026-09-21. Five scenarios described a migration error class and
  # a subclass of it, and the declaration replaced both: a failure is a `Fault`
  # that comes back by return, carrying its kind, what went wrong and what to do
  # instead. Which class inherits from which is not something a caller can see.
  # What stopped the deploy, and whether the database was left part way through,
  # is.

  @precommit
  Scenario: a stopped migration says which operation stopped it and what the engine said
    Given a database whose disk fills partway through the migration
    When a caller opens it
    Then a fault comes back instead of a handle
    And it names the operation it stopped on and what the engine said
    And it says what to do instead

  @precommit
  Scenario: a database left part way through says a person has to look at it
    Given a migration that stops on an engine that cannot undo what already ran
    When a caller opens it
    Then the fault says the database is part way through its migration
    And it tells the operator to look at the database by hand
