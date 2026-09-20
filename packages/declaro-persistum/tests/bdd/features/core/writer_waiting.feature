@bdd @specification
Feature: writer_waiting -- Whether a writer wants the connection
  `docs/architecture.md:308`: the sync pass is dropped when a writer is waiting,
  because politeness toward writers is the whole of why sync is opportunistic.

  Scenario: a waiting writer drops the pass
    Given one write in the room
    Then a sync pass would be dropped

  Scenario: an empty room lets it run
    Given nothing in the room
    Then a sync pass may run
