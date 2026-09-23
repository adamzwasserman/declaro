@bdd @specification
Feature: writer_waiting -- Whether a writer wants the connection
  `docs/architecture.md:308`: the sync pass is dropped when a writer is waiting,
  because politeness toward writers is the whole of why sync is opportunistic.

  A writer is waiting when the crew is holding a write nobody has answered yet.

  Scenario: a waiting writer drops the pass
    Given one write handed to the crew
    Then a sync pass would be dropped

  Scenario: a crew holding nothing lets it run
    Given nothing handed to the crew
    Then a sync pass may run

  Scenario: a write that has been answered is not a writer waiting
    Given one write handed to the crew and its outcome handed back
    Then a sync pass may run
