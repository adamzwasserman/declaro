@bdd @specification
Feature: query_begin_concurrent -- What lets drainers overlap instead of queue
  `docs/architecture.md:220`: a deposited write opens so that drainers overlap.
  Without that the crew is ordinary locking, and N drainers are N coroutines
  waiting behind one writer.

  THE CLAIM IS TURSO'S ALONE, and the first scenario says so. Turso is the one
  engine of the three that lets a second writer open while the first is still
  open; stock SQLite makes it wait and PostgreSQL has no equivalent to name.
  This function is handed a connection and nothing that says which engine it
  belongs to, so the scenario names the engine rather than the statement.

  # RESTATED 2026-09-21. This said "it opened the write concurrently", which
  # names the statement the function sends and is true of no engine but one. A
  # caller cannot see a statement. What a caller sees is that a second drainer
  # is not turned away while the first is writing.
  Scenario: two drainers write at once and neither is turned away
    Given a Turso database and two drainers
    When both take a write off the room while the other is still writing
    Then both writes land
    And neither was refused for being the second one to start

  Scenario: it runs on the drainer's own connection
    When two drainers each take a write
    Then each opened its write on the connection it holds

  Scenario: a conflict at commit is not a refusal
    When two drainers write the same row and one conflicts at commit
    Then the conflict is retried
    And the other write stands
