@bdd @specification
Feature: query_ask -- The door a caller asks a question or declares a write at
  One door, four spellings. Which grammar was used arrives with the call, because
  the entry point a caller reached for is the shape: nothing inspects a call to
  work it out.

  Scenario Outline: every grammar reaches the same door
    When a caller asks for people named "bob" in the <grammar> grammar
    Then the same declaration was built

    Examples:
      | grammar    |
      | pseudo_sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario: a read answers with rows
    When a caller asks for every person
    Then rows come back as instances of the model

  Scenario: a write answers with a ticket
    When a caller declares one new person
    Then a ticket comes back and the caller does not wait

  Scenario: a column no model declares is refused here, not by the engine
    When a caller asks for the column "nmae"
    Then it is refused naming the model and the columns it declares
    And nothing reached the database
