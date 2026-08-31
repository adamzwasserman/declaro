@bdd @specification
Feature: A statement is built once for a question that does not change
  The library turns a declaration into a statement. The same declaration asked a
  thousand times is a thousand identical statements, and building them is work nobody
  asked for.

  THIS IS NOT A CACHE OF RESULTS. Rows are never held. What is held is the rendering
  of a question into a statement, which is a pure function of the question and the
  engine, so holding it can never answer with stale data.

  Scenario: the same question renders once
    Given a question about Person narrowed by name
    When it is asked a hundred times with different values
    Then the statement was built once

  Scenario: a different question renders again
    Given a question about Person narrowed by name
    And a question about Person narrowed by age
    Then the two render separately

  Scenario: the same question on a different engine renders again
    Given a question about Person narrowed by name
    When it is asked on sqlite and on postgresql
    Then each engine got its own statement

  Scenario: values are never held
    Given a question asked with the value "bob" and then with "alice"
    Then only the statement was reused
    And each asking bound its own value

  Scenario: no row is ever held
    When any question is asked twice with the same value
    Then both askings read the database

  Scenario: what is held is bounded
    When more distinct questions are asked than the library will hold
    Then it holds a bounded number and the rest render again
    And nothing grows without limit
