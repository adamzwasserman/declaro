@bdd @specification
Feature: progress_line -- A shutdown line fit to be the last one
  Every line names the database and the time elapsed, because "replicating"
  alone does not tell progress from a stall, and does not say which database is
  holding up the exit.

  Nothing runs after the kill, so every line has to be fit to be the last one.

  Scenario: a line names the database and the elapsed time
    When a shutdown has been running for four seconds
    Then the line says which database and says four seconds

  Scenario: until the push ends, every line says the data has not arrived
    When a shutdown has been running for four seconds
    Then a process killed after that line leaves a true last line

  Scenario: the last line after success says it arrived
    When the push finishes
    Then the final line says the primary has everything

  # A database carries one thing that names it: where it is. An engine is shared
  # between databases, a connection may never be read, and a crew belongs to
  # whichever database made it. A line naming anything else would name the same
  # thing for every database exiting at once, which is the stall these lines
  # exist to tell apart from progress.
  Scenario: two databases exiting together are told apart
    Given two databases shutting down from different places
    Then their lines name different databases
