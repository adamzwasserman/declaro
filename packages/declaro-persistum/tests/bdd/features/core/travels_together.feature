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
