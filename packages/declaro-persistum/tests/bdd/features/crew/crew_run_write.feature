@bdd @specification
Feature: crew_run_write -- Running one deposited write on the drainer's connection
  A drainer writes and never reads, on the one connection it holds for its life,
  which is why this takes that connection rather than the database.

  Scenario: it runs on the connection it was given
    When a drainer runs a write
    Then it ran on that drainer's connection

  Scenario: it says how many rows it touched
    When a write changes two rows
    Then the answer is two

  Scenario: a contention failure is told apart from a refusal
    When a write conflicts at commit
    Then the fault says it is worth trying again

  # THE CONTROL, ADDED 2026-09-24 WITH THE FUNCTION. The scenario above passes
  # on a runner that calls every failure a conflict, and that runner spends a
  # whole budget re-running a write the engine will refuse every time. "Told
  # apart" needs both halves said.
  Scenario: a refusal is told apart from a contention failure
    When a write names a column no model declares
    Then the fault says it is not worth trying again
