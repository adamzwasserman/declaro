@bdd @specification
Feature: query_run_together -- Several writes that land together, or not at all
  `HOW-IT-SHOULD-WORK.md`: declare several together and they arrive together.
  `docs/architecture.md:150`: two deposits are two independent writes, and the
  first stays landed when the second fails, so holding the connection is the
  only rollback there is.

  Scenario: all of them land
    When three rows are declared together and all three are accepted
    Then all three are in the database

  Scenario: one refusal undoes the others
    When three rows are declared together and the second violates a constraint
    Then none of the three is in the database
    And the fault names the second

  Scenario: it does not go through the room
    When three rows are declared together
    Then no ticket was handed out

  # ------------------------------------------------------------------
  # FOLDED IN FROM `running/several_writes_travel_together.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # "The library decides whether several writes travel as one transaction" is true for a
  # list handed to one call. Two separate calls have no way to say they belong together,
  # and that is the common case: change a row and remove another, both or neither.

  Scenario: a list handed to one call lands together
    When a caller declares two rows exist, as a list
    And the second cannot be written
    Then neither is in the database

  Scenario: two separate calls are two separate writes
    When a caller declares one row, and then another
    And the second fails
    Then the first is in the database

  Scenario: a group is declared, and the calls inside it travel together
    When a caller declares a group holding a change and a removal
    And the removal fails
    Then the change did not happen either

  Scenario: a group reports what it did as one answer
    When a group of three writes succeeds
    Then the answer says three rows changed
    And it is one answer, not three

  Scenario: a group that cannot start says so before writing anything
    Given a group naming a column no model declares
    When it is declared
    Then it is refused before any of its writes runs

  Scenario: a read inside a group sees the group's own writes
    When a caller declares a group that adds a row and then counts rows
    Then the count includes the row the group added

  Scenario: a group is not a connection
    When a caller declares a group
    Then nothing in the group's declaration is a connection
    And the caller never holds one

  # ------------------------------------------------------------------
  # FOLDED IN FROM `writers/what_a_connection_must_offer.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As code that scopes a write and has to undo it when the write fails
  # I want the methods I call named in a contract
  # So that a method nobody promised cannot be the thing a rollback rests on
  # THE PROTOCOL IS THE CONTRACT, and its method names ARE the interface. They
  # cannot be collapsed into one function over a table: a Protocol is a set of
  # named members, and generating them would leave mypy with nothing to check.
  #
  # WHAT IS TESTABLE IS THAT THE CONTRACT COVERS WHAT THE CODE CALLS. The
  # Protocol once named `execute` and `commit` only, so the rollback half of
  # the transaction scope rested on a method no contract required. mypy caught
  # it; these scenarios keep it caught if the Protocol is ever trimmed.
  # @precommit

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
