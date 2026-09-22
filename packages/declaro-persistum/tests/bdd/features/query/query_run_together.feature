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

  Scenario: a group is not a connection
    When a caller declares a group
    Then nothing in the group's declaration is a connection
    And the caller never holds one

  # ------------------------------------------------------------------
  # FOUR SCENARIOS WERE DELETED HERE ON 2026-09-21, and this comment is what is
  # left of them. Three came from `writers/what_a_connection_must_offer.feature`
  # and asked which driver methods a Protocol lists: `execute`, `commit`,
  # `rollback`. A list of driver method names is the mechanism, not the result.
  # The declaration types a connection as a handle that is held, passed onward
  # and never read, so nothing in this package may name a method on one, and a
  # scenario that reads the contract cannot be restated as anything a caller
  # observes. Swapping the driver honestly would break all three while the
  # behaviour they exist for -- one refusal undoes the others -- is untouched,
  # and that behaviour already has a scenario at the top of this file.
  #
  # The fourth, "a read inside a group sees the group's own writes", asked for
  # what every SQL engine does by definition inside one transaction. The
  # declaration gives this function an int, so a group's answer is a count of
  # rows changed and a read inside it has no way back to the caller.
