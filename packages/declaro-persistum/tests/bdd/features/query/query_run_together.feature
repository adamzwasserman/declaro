@bdd @specification
Feature: query_run_together -- Several writes that land together, or not at all
  `HOW-IT-SHOULD-WORK.md`: declare several together and they arrive together.
  `docs/architecture.md:150`: two deposits are two independent writes, and the
  first stays landed when the second fails, so holding the connection is the
  only rollback there is.

  Scenario: all of them land
    When three rows are declared together and all three are accepted
    Then all three are in the database

  Scenario: one refusal undoes the others
    When three rows are declared together and the second violates a constraint
    Then none of the three is in the database
    And the fault names the second

  Scenario: it does not go through the room
    When three rows are declared together
    Then no ticket was handed out
