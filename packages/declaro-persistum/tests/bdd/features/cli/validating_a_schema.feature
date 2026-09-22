@bdd
Feature: What `declaro validate` tells an operator about their schema files
  As someone about to run a migration
  I want validate to read my actual schema files and say what is wrong
  So that I find a broken reference before it reaches a database

  # EVERY SCENARIO RUNS THE COMMAND ON REAL FILES. A schema is TOML on disk,
  # and each scenario below writes some and points validate at it, then reads
  # the exit code and the terminal.
  #
  # WHY THIS FILE EXISTS. Ten tests replaced the schema reader with a fake
  # that handed back a dictionary, so nothing ever read a file. A schema
  # that will not load, a bad reference and an unknown type are all things a
  # real file can say, which I checked before writing any of this.
  #
  # WHAT WAS DELETED, 2026-09-21. Five scenarios took the single exit-code card
  # `docs/design/persistum-cli.hd` declares, `fn exit_code_for`, and asked five
  # private helpers of a `cli/main.py` the declaration does not have. Each one
  # repeated a scenario above it one level down: the strict warning exiting two
  # and the clean schema exiting zero are already stated as runs of the command.

  @precommit
  Scenario: a schema that reads and validates exits zero
    Given a folder holding a table with one column
    When validate runs on it
    Then the exit code is 0
    And it says the schema is valid

  @precommit
  Scenario: a folder with no schema files in it exits one
    Given an empty folder
    When validate runs on it
    Then the exit code is 1
    And it says the schema would not load

  @precommit
  Scenario: a folder that is not there exits one
    Given a folder that does not exist
    When validate runs on it
    Then the exit code is 1
    And it says the schema would not load

  @precommit
  Scenario: a column pointing at a table nobody declared exits one
    Given a folder holding a column that references a table nobody declared
    When validate runs on it
    Then the exit code is 1
    And it names the table that is missing

  @precommit
  Scenario: an unknown column type is a warning and still exits zero
    Given a folder holding a column whose type nobody recognizes
    When validate runs on it
    Then the exit code is 0
    And it warns about the type

  @precommit
  Scenario: the same warning under --strict exits two
    Given a folder holding a column whose type nobody recognizes
    When validate runs on it in strict mode
    Then the exit code is 2

  @precommit
  Scenario: --strict changes nothing when there are no warnings
    Given a folder holding a table with one column
    When validate runs on it in strict mode
    Then the exit code is 0

  @precommit
  Scenario: an error beats a strict warning
    Given a folder holding both a bad reference and an unrecognized type
    When validate runs on it in strict mode
    Then the exit code is 1

  @precommit
  Scenario: --verbose lists each table and how many columns it has
    Given a folder holding a table with one column
    When validate runs on it verbosely
    Then the exit code is 0
    And it lists the table and its column count
