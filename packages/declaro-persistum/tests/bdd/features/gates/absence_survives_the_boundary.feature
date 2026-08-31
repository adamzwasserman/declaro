@bdd @specification
Feature: A named absence survives leaving the process
  Inside Python, absence is a named value you have to look at. The moment an answer
  leaves the process, into a response body or a queue or a log, that name has to
  survive or the distinction it exists to keep is lost at the edge.

  `None` is the wrong shape inside Python because a nullable column holds `None` too.
  `null` in a response body is wrong for exactly the same reason.

  Scenario: an absence is not the same as a row whose column is null
    Given a person whose name is null
    And no person at all under another question
    Then the two answers are different values

  Scenario: an absence turned into a response body stays distinguishable
    When an absence is rendered for a caller outside the process
    Then a reader can tell it from a row carrying null

  Scenario: an absence never becomes an empty list
    When one row is asked for and there is none
    Then the answer is the absence, not an empty collection

  Scenario: an empty collection never becomes an absence
    When many rows are asked for and there are none
    Then the answer is an empty collection, not the absence

  Scenario: an absence cannot be used as a row by accident
    When a caller reads a column off an absence
    Then it refuses rather than answering None

  Scenario: the absence is one value, not one per module
    When every place that answers with absence is read
    Then all of them answer with the same value
