@bdd
Feature: What the command line does with what a command returns or raises
  As an operator whose command failed
  I want the exit code and the message to say what happened
  So that a script calling this can tell the three kinds of ending apart

  # ONE SCENARIO PER FUNCTION in the dispatch path of `cli/main.py`.
  #
  # WHY THIS FILE EXISTS. Eleven tests answered these questions by replacing
  # `cmd_diff` on the module with a fake that raised or returned a code. That
  # is a mock: the real function is prevented from running. The reason it was
  # the only way is that `_dispatch` read its command table off module scope,
  # so the only handle a test had on "a command that raises" was the module
  # attribute.
  #
  # `_dispatch` takes the table as an argument now. Handing it a table is not
  # replacing anything of ours: it is calling the function with its input,
  # exactly as `Database` carries the callables it needs rather than reaching
  # for them by name.

  @precommit
  Scenario: _dispatch returns the code the command returned
    Given a command that returns 2
    When _dispatch runs it
    Then the exit code is 2

  @precommit
  Scenario: _dispatch turns an interrupt into 130
    Given a command that raises KeyboardInterrupt
    When _dispatch runs it
    Then the exit code is 130
    And "Aborted" was printed

  @precommit
  Scenario: _dispatch turns any other failure into 1 and says what it was
    Given a command that raises RuntimeError with "the disk went away"
    When _dispatch runs it
    Then the exit code is 1
    And "the disk went away" was printed

  @precommit
  Scenario: _dispatch refuses a command its table does not hold
    Given a command named "invent" that no table holds
    When _dispatch runs it
    Then the exit code is 1

  @precommit
  Scenario: _print_traceback prints where the failure came from
    When _print_traceback runs inside a failure
    Then a traceback was printed

  @precommit
  Scenario: _keep_the_traceback prints nothing at all
    When _keep_the_traceback runs inside a failure
    Then nothing was printed

  # THE ROUTING TABLE IS DATA, so the question "does this subcommand reach its
  # own implementation" is answered by reading it. Four tests answered it by
  # replacing each handler in turn with a recorder and running the program.

  @precommit
  Scenario Outline: every subcommand an operator can type reaches its own runner
    When the command table is asked for "<typed>"
    Then it answers the runner named "<runner>"

    Examples:
      | typed          | runner              |
      | diff           | _run_diff           |
      | apply          | _run_apply          |
      | snapshot       | _run_snapshot       |
      | migrate-remote | _run_migrate_remote |
      | validate       | _run_validate       |
      | generate       | _run_generate       |

  @precommit
  Scenario: the table holds every subcommand the parser offers and no others
    When the parser's subcommands are compared with the command table
    Then the two name exactly the same commands

  # WHICH COMMANDS NEED A DATABASE. `validate` reads schema files off disk and
  # `migrate-remote` is told its target another way, so neither is refused for
  # want of -c. One test asserted this by replacing `cmd_validate` with a fake
  # that returned zero, which proves the fake returned zero.

  @precommit
  Scenario Outline: a command that reads no database runs without one
    When _run_command is asked whether "<typed>" can run with no connection
    Then the answer is "<verdict>"

    Examples:
      | typed          | verdict  |
      | validate       | runs     |
      | migrate-remote | runs     |
      | diff           | refused  |
      | apply          | refused  |
      | snapshot       | refused  |
      | generate       | refused  |

  @precommit
  Scenario: _refuse_missing_connection names both ways to supply one
    When _refuse_missing_connection answers a command that needed a database
    Then the exit code is 1
    And "DECLARO_DATABASE_URL" was printed

  # THE TABLE THAT PICKS BETWEEN THEM, which is a different claim from what
  # each half does. Testing only the halves let a swap of the two entries pass
  # unnoticed, measured 2026-08-27 by making the verbose case print nothing.

  @precommit
  Scenario: -v picks the half that prints the stack
    When the traceback table is asked what verbose does
    Then it answers "_print_traceback"

  @precommit
  Scenario: without -v it picks the half that prints nothing
    When the traceback table is asked what quiet does
    Then it answers "_keep_the_traceback"
