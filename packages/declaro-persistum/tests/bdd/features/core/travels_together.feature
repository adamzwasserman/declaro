@bdd @specification
Feature: travels_together -- Which writes have to land together
  `add` carrying one row is a deposit and a ticket. `add` carrying several is a
  transaction, because the caller asked for all of them or none.

  Scenario: one row is deposited
    Given a declaration carrying one row
    Then it does not have to travel together

  Scenario: several rows travel together
    Given a declaration carrying three rows
    Then they have to land together

  Scenario: a read never travels together
    Given a declaration asking for every person
    Then the question does not arise

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
