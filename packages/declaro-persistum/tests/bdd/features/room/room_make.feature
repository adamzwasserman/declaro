@bdd @specification
Feature: room_make -- An empty waiting room
  `docs/architecture.md:140`: a room holds the arrival order and the callers
  waiting on tickets, and both are empty whenever nobody is mid-flight. It is a
  waiting room, not a store: nothing about it survives the process, because every
  write has a caller holding its ticket.

  Scenario: a new room holds nothing
    When a room is made
    Then it has no arrivals and nobody waiting
