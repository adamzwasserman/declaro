@bdd
Feature: What an operator gets back when a command returns, is interrupted, or fails
  As an operator whose command failed
  I want the exit code and the message to say what happened
  So that a script calling this can tell the three kinds of ending apart

  # WHAT THIS FILE IS ABOUT. `docs/design/persistum-cli.hd` declares one door,
  # `boundary_in fn command_run`, which takes what an operator typed and answers
  # with an exit code. Everything below runs that door and reads the two things
  # an operator can see: the code the shell gets, and the lines on the terminal.
  #
  # WHY IT WAS REWRITTEN, 2026-09-21. Every scenario here used to name a private
  # helper of a `cli/main.py` the declaration does not have: `_dispatch`,
  # `_print_traceback`, `_run_diff`, `_refuse_missing_connection`. Two of them
  # asserted that a lookup answered a helper's name, which is a fact about a
  # table rather than about a run, and one listed a `migrate-remote` command the
  # declaration's five command words do not include. A reader cannot type a
  # helper, so a helper is not a result.
  #
  # THE FIVE COMMANDS are diff, apply, validate, snapshot and generate.

  @precommit
  Scenario: a command's own code comes back as the exit code
    Given a command that returns 2
    When an operator runs it
    Then the exit code is 2

  @precommit
  Scenario: an interrupt is 130 and says the run was aborted
    Given a command that is interrupted at the keyboard
    When an operator runs it
    Then the exit code is 130
    And "Aborted" was printed

  @precommit
  Scenario: any other failure is 1 and the operator reads what broke
    Given a command that fails with "the disk went away"
    When an operator runs it
    Then the exit code is 1
    And "the disk went away" was printed

  @precommit
  Scenario: a word that is not one of the five commands is refused
    When an operator types "invent"
    Then the exit code is 1
    And the refusal names diff, apply, validate, snapshot and generate

  # HOW MUCH OF A FAILURE THE OPERATOR READS is the one thing -v changes. Two
  # scenarios used to assert which helper the verbose case selected, which is
  # the same claim one level down and survives no rename.

  @precommit
  Scenario Outline: -v decides whether the operator also reads where a failure came from
    Given a command that fails with "the disk went away"
    When an operator runs it <switch>
    Then the exit code is 1
    And "the disk went away" was printed
    And the stack of the failure <stack>

    Examples:
      | switch     | stack           |
      | with -v    | was printed     |
      | without -v | was not printed |

  # NO COMMAND WORD IS ACCEPTED AND THEN IGNORED. The old scenario answered this
  # by comparing the parser's subcommands against a routing table, which is two
  # internal structures agreeing with each other. Running each command says it.

  @precommit
  Scenario Outline: every command an operator can type does its own work
    Given a database the command can reach
    When an operator types "<typed>"
    Then the exit code is 0
    And the operator is told "<report>"

    Examples:
      | typed    | report                             |
      | diff     | what a migration would change      |
      | apply    | what the migration changed         |
      | validate | whether the schema files are sound |
      | snapshot | where the schema was written       |
      | generate | where the types were written       |

  # WHICH COMMANDS NEED A DATABASE. `validate` reads schema files off disk, so
  # it is the one command a missing url does not stop.

  @precommit
  Scenario Outline: only a command that talks to a database is refused for want of one
    Given a folder holding a table with one column
    And no url was typed and none is in the environment
    When an operator types "<typed>"
    Then the exit code is <code>

    Examples:
      | typed    | code |
      | validate | 0    |
      | diff     | 1    |
      | apply    | 1    |
      | snapshot | 1    |
      | generate | 1    |

  @precommit
  Scenario: a command refused for want of a database names both ways to give it one
    Given no url was typed and none is in the environment
    When an operator types "diff"
    Then the exit code is 1
    And the refusal names "-c" and "DECLARO_DATABASE_URL"
