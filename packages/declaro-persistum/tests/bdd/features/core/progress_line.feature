@bdd @specification
Feature: progress_line -- A shutdown line fit to be the last one
  Every line names the database and the time elapsed, because "replicating"
  alone does not distinguish progress from a stall and does not say which
  database is holding up the exit.

  Scenario: a line names the database and the elapsed time
    When a shutdown has been running for four seconds
    Then the line says which database and says four seconds

  Scenario: until the push ends, every line says the data has not arrived
    Then a process killed after any line leaves a true last line

  Scenario: the last line after success says it arrived
    When the push finishes
    Then the final line says Turso has everything

  # ADDED 2026-09-21, while building the function. A database carries no name of
  # its own: it holds an engine, a driver handle nobody may open, a room, a
  # shutdown policy and the primary it pushes to. So "which database" can only
  # be the primary, and a line that named anything else would name the same
  # thing for every database exiting at once, which is the stall this feature
  # exists to tell apart from progress.
  Scenario: two databases exiting together are told apart
    Given two databases shutting down against different primaries
    Then their lines name different databases
