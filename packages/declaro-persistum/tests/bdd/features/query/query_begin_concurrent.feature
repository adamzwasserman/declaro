@bdd @specification
Feature: query_begin_concurrent -- What lets drainers overlap instead of queue
  `docs/architecture.md:220`: BEGIN CONCURRENT is mandatory on a deposited write.
  Without it the crew is ordinary locking, and N drainers are N coroutines
  waiting behind one writer.

  Scenario: every deposited write opens this way
    When a drainer takes a write off the room
    Then it opened the write concurrently before running it

  Scenario: it runs on the drainer's own connection
    When two drainers each take a write
    Then each opened its write on the connection it holds

  Scenario: a conflict at commit is not a refusal
    When two drainers write the same row and one conflicts at commit
    Then the conflict is retried
    And the other write stands
