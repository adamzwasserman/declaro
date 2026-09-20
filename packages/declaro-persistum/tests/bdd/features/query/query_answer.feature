@bdd @specification
Feature: query_answer -- Turning a caller's question into an answer
  One orchestrator. It builds the declaration from whichever grammar was used,
  refuses undeclared columns, compiles the statement once for this engine, and
  then either runs it or deposits it.

  Scenario: a read runs now
    When a caller asks for every person
    Then the statement ran against the database
    And the rows came back shaped as the model

  Scenario: a write is deposited
    When a caller declares one new person
    Then the write went to the room
    And a ticket came back

  Scenario: several rows declared together do not take a ticket
    When a caller declares three people together
    Then they run as one transaction
    And nothing was deposited

  Scenario: the values never reach the statement text
    When a caller asks for people named "bob"
    Then the statement carries a placeholder
    And "bob" came back beside it for the driver to bind
