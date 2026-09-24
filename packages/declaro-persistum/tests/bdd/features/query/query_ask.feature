@bdd @specification
Feature: query_ask -- The door a caller asks a question or declares a write at
  One door, four spellings. Which grammar was used arrives with the call, because
  the entry point a caller reached for is the shape: nothing inspects a call to
  work it out.

  # TRIMMED 2026-09-24, to what this card answers. It takes a database, a model,
  # a grammar and a question, and hands back an answer or a fault. What went:
  #
  # Twelve scenarios comparing what four grammars build -> the four
  # `declaration_from_*` features, which each own their own phrasing. This door
  # picks a builder and reads nothing about what the builder makes.
  #
  # "a ticket comes back and the caller does not wait" -> `Answer` has no
  # Ticket. A caller holding one had to come back for the outcome and every
  # caller did the same thing with it, so the waiting lives in
  # `orchestrator fn query_answer` and the caller gets the outcome itself.

  Scenario Outline: every grammar reaches the same door
    When a caller asks for every person in the <grammar> grammar
    Then the rows come back

    Examples:
      | grammar    |
      | pseudo_sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario: a read answers with rows
    When a caller asks for every person
    Then the rows come back

  Scenario: a write answers with how many rows it touched
    When a caller declares one new person
    Then the answer is one, and the person is in the table

  Scenario: a column no model declares is refused here, not by the engine
    When a caller asks for the column "nmae"
    Then it is refused naming the model and the columns it declares
    And nothing reached the database

  # A DOOR NEVER RAISES. A caller asking a question gets an answer or a fault,
  # and an exception reaching them is this package handing its own defect to
  # somebody who asked for rows.
  Scenario: a question that goes wrong comes back as a fault
    When a caller asks against a database whose connection has been closed
    Then a fault comes back and nothing was raised
