@bdd
Feature: What this package requires of a database connection
  As code that scopes a write and has to undo it when the write fails
  I want the methods I call named in a contract
  So that a method nobody promised cannot be the thing a rollback rests on

  # THE PROTOCOL IS THE CONTRACT, and its method names ARE the interface. They
  # cannot be collapsed into one function over a table: a Protocol is a set of
  # named members, and generating them would leave mypy with nothing to check.
  #
  # WHAT IS TESTABLE IS THAT THE CONTRACT COVERS WHAT THE CODE CALLS. The
  # Protocol once named `execute` and `commit` only, so the rollback half of
  # the transaction scope rested on a method no contract required. mypy caught
  # it; these scenarios keep it caught if the Protocol is ever trimmed.

  @precommit
  Scenario Outline: the DB-API contract names what a write block calls
    When the DB-API connection contract is read
    Then it requires "<method>"

    Examples:
      | method   |
      | execute  |
      | commit   |
      | rollback |

  @precommit
  Scenario: the asyncpg contract names what its own write block calls
    When the asyncpg connection contract is read
    Then it requires "execute"

  @precommit
  Scenario: every method the transaction scope calls is in a contract
    When the transaction scope is read for the methods it calls on a connection
    Then every one of them is named by the DB-API contract
