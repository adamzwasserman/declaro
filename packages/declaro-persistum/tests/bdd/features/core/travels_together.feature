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
