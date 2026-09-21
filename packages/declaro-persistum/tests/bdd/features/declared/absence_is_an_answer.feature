@bdd @specification
Feature: Absence is an answer
  A row that is not there is a real result, not a failure and not an empty space. The
  library hands back a named absence and the caller has to look at it.

  Section "Absence is an answer" of HOW-IT-SHOULD-WORK.md.

  Background:
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol

  Scenario: one row that is not there comes back as a named absence
    When a caller asks for the one person named "nobody"
    Then it is given a named absence
    And the absence is not None

  Scenario: the absence cannot be mistaken for a row
    When a caller asks for the one person named "nobody"
    Then reading a column off the answer refuses
    And nothing silently stands in for the row that was missing

  Scenario: no rows is an empty answer, not an absence
    When a caller asks for every person named "nobody"
    Then it is given no people
    And that is a different answer from the named absence

  # A CARD, NOT A CATEGORY. Rewritten 2026-09-21: these said "a teardown", which
  # names nothing the declaration has, so whoever bound them would pick a card
  # and could pick the one where it is false. `boundary_in fn lifecycle_stop` is
  # opened by a signal and has no caller to return anything to, which is why
  # `fn progress_line` and `boundary_out fn log_write` exist. A caller's own
  # question is where a failure has somewhere to go.
  Scenario: a question that cannot be answered comes back as a failure
    Given a question the engine refuses
    When a caller asks it
    Then the failure comes back as a value to the caller
    And it is not written to a log instead

  Scenario: a failure is never reported as success
    Given a question the engine refuses
    When a caller asks it
    Then the caller can tell that answer from one that worked
