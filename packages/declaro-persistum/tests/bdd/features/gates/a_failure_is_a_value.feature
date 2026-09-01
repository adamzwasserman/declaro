@bdd @specification
Feature: A failure is a value, everywhere
  "A function that cannot do its job returns that fact" is the rule. Which functions
  return a failure and which raise is not written down, so a caller cannot tell what
  to expect, and the answer differs module by module.

  THE LINE IS THE BOUNDARY. Inside, a failure is a value: the caller is the library
  itself and it can act on one. At the edge, a failure the caller must not ignore is
  raised, because a returned value can be dropped and an exception cannot.

  Scenario: an interior function hands its failure back
    Given an interior function that cannot finish
    Then it returns what stopped it
    And it raises nothing

  Scenario: an interior function never writes a failure instead of returning it
    Given an interior function that cannot finish
    Then nothing about the failure was written anywhere

  Scenario: a boundary raises what a caller must not ignore
    Given a caller asking for a row in a database that is not there
    Then it is raised, not returned

  Scenario: a returned failure is not shaped like a result
    Given a function that returns a failure
    Then a caller cannot mistake it for the answer

  Scenario: a teardown failure rides out on the failure already traveling
    Given work that failed, and a teardown that also failed
    Then the caller is told about both
    And the first failure is still the one it catches on

  Scenario: which functions raise is declared, not discovered
    When every public function is read
    Then each says whether it raises or returns its failures
