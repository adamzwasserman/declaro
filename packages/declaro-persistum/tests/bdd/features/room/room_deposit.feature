@bdd @specification
Feature: room_deposit -- Leaving a write behind and taking a ticket
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
