@bdd
Feature: A model declares, and does nothing else
  A Pydantic model is the only description of the database, and the loader reads two
  things off it: the table name from `@table`, and the columns from `model_fields`.
  Everything else in the class is invisible to the library.

  THAT IS WHY A VALIDATOR IS A DEFECT AND NOT A STYLE. A rule written as a validator
  holds in Python and never reaches the database, so the constraint exists in one of
  the two places it should. Which one wins depends on whether the write came through
  your code or a psql prompt. It is the two-descriptions bug hiding inside the one
  file that exists to be the single description.

  THE REFUSAL HANDS BACK THE DECLARATION. Every habit here has a declared form, and
  the message prints it ready to paste. A checker that only says no leaves the writer
  to guess, and the guess is usually the escape hatch.

  Scenario: a model that only declares is accepted
    Given a model declaring a table with columns and nothing else
    Then it is accepted

  Scenario: a validator is refused, and the check is offered
    Given a model carrying a validator
    Then it is refused
    And the message names the validator
    And the message offers a declared check

  Scenario: a method is refused, and named as not a column
    Given a model carrying a method
    Then it is refused
    And the message says a computed value is not a column

  Scenario: a property is refused the same way as a method
    Given a model carrying a property
    Then it is refused
    And the message says a computed value is not a column

  Scenario: a Python default is accepted, because the loader reads it
    Given a model whose column carries a Python default
    Then it is accepted

  Scenario: inheriting from anything but BaseModel is refused
    Given a model inheriting from another model
    Then it is refused
    And the message says the columns must be visible in this file

  Scenario: every habit found is reported, not just the first
    Given a model carrying a validator and a method
    Then it is refused
    And both are named

  Scenario: the rules are a table, so the checker knows nothing about validators
    When the rules are read
    Then each row carries what to look for and what to say
    And the checker names none of them
