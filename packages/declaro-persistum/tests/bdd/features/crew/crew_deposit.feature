@bdd @specification
Feature: crew_deposit -- Handing a write to the crew and taking a ticket
  `docs/architecture.md:201`: the caller deposits and gets a ticket back at once,
  and a crew of drainers empties the queue afterwards.

  Scenario: a deposit returns before the write is done
    When a caller deposits a write
    Then it is handed a ticket without waiting for the write

  Scenario: a write with no statement is refused at the door
    When a caller deposits a write with nothing to run
    Then it is refused before it takes a ticket
    And the crew is given nothing to do

  Scenario: a deposited write reaches the engine exactly once
    When a caller deposits one write and the crew drains once
    Then the engine was asked to run it once

  # Added 2026-09-22. One deposit cannot show either of these, and both lose a
  # caller's write: two callers holding one ticket collect each other's outcome,
  # and two writes drained out of order land in the database back to front.
  Scenario: two writes keep their order and each caller keeps its own ticket
    When two callers each deposit a write
    Then each caller is given a ticket of its own
    And the crew is handed the two writes in the order they arrived
