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

  Scenario: a function that cannot do its job returns that fact
    Given a teardown that cannot finish
    When the library runs it
    Then the failure comes back as a value to the caller
    And it is not written to a log instead

  Scenario: a failure is never reported as success
    Given a teardown that cannot finish
    When the library runs it
    Then the caller can tell that from a teardown that worked
