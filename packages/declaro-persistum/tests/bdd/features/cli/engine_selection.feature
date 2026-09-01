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
  # ONE SCENARIO PER FUNCTION in the engine-selection path of `cli/main.py`.
  # Every one of these is pure: a value in, a value out.
  #
  # WHY THIS FILE EXISTS. Two tests used to answer these questions by replacing
  # a command handler with a fake, running the whole program, and reading a
  # variable the fake had captured. The answer they were reaching for is what
  # `_chosen_engine` returns, so the fake was standing between the test and a
  # function that already hands the value back.

  @precommit
  Scenario: _keep_given_engine keeps what the operator typed
    When _keep_given_engine is given "postgresql" alongside the url "mysql://host/db"
    Then the engine is "postgresql"

  @precommit
  Scenario: _detect_from_url reads the engine off the url
    When _detect_from_url is given no engine alongside the url "sqlite:///x.db"
    Then the engine is "sqlite"

  @precommit
  Scenario: _chosen_engine prefers what the operator typed over the url
    When an operator types --engine "postgresql" with the url "mysql://host/db"
    Then the engine is "postgresql"

  @precommit
  Scenario: _chosen_engine reads the url when the operator typed no engine
    When an operator types no --engine with the url "sqlite:///x.db"
    Then the engine is "sqlite"

  @precommit
  Scenario: _chosen_engine answers nothing when there is no url to read
    When an operator types no --engine with no url
    Then there is no engine

  @precommit
  Scenario: detect_engine refuses a url naming an engine nobody supports
    When detect_engine is given the url "mysql://host/db"
    Then it refuses and says to pass --engine

  @precommit
  Scenario: _refuse_unknown_prefix says what the operator can do about it
    When _refuse_unknown_prefix is given no prefix matches at all
    Then it refuses and says to pass --engine

  @precommit
  Scenario: _the_named_engine hands back the engine the prefix named
    When _the_named_engine is given the prefix matches "sqlite"
    Then the engine is "sqlite"

  @precommit
  Scenario: get_connection_string prefers the url the operator typed
    When an operator types the url "sqlite:///typed.db" with "sqlite:///env.db" in the environment
    Then the connection string is "sqlite:///typed.db"

  @precommit
  Scenario: get_connection_string falls back to the environment
    When an operator types no url with "sqlite:///env.db" in the environment
    Then the connection string is "sqlite:///env.db"
