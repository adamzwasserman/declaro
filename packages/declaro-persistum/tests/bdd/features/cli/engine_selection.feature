@bdd
Feature: Which engine the command line decides to talk to
  As an operator running a migration command
  I want the engine chosen from what I typed, or read off the URL when I said nothing
  So that a command never runs against an engine nobody picked

  # THE OPERATOR MAY STILL NAME AN ENGINE HERE, and only here. This is the
  # command line, which is a boundary: argparse's `choices=` admits one of
  # three words from outside and the interior then trusts it. No library
  # function takes one, and `tests/bdd/features/opening/engine_of.feature`
  # specifies how the engine is worked out when nobody says.
  #
  # WHY IT WAS REWRITTEN, 2026-09-21. `docs/design/persistum-cli.hd` settles the
  # engine in one card, `fn chosen_engine`, which takes what the operator typed
  # and the url and answers with an engine or a refusal. Eight scenarios used to
  # spread that one decision over six private helpers of a `cli/main.py` the
  # declaration does not have, and two of those asked the same three questions
  # again one level further down. There are three answers to have, so there are
  # three scenarios.

  @precommit
  Scenario: the engine an operator names wins over the url
    When an operator types --engine "postgresql" with the url "mysql://host/db"
    Then the command talks to postgresql

  @precommit
  Scenario: the url decides when the operator names no engine
    When an operator types no --engine with the url "sqlite:///x.db"
    Then the command talks to sqlite

  @precommit
  Scenario: a url naming an engine nobody ships is refused
    When an operator types no --engine with the url "mysql://host/db"
    Then it is refused and the refusal says to pass --engine
    And the refusal names sqlite, postgresql and turso

  @precommit
  Scenario: the url an operator typed wins over the environment
    When an operator types the url "sqlite:///typed.db" with "sqlite:///env.db" in the environment
    Then the database opened is "sqlite:///typed.db"

  @precommit
  Scenario: an operator who types no url gets the one in the environment
    When an operator types no url with "sqlite:///env.db" in the environment
    Then the database opened is "sqlite:///env.db"
