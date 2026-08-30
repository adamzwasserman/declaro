@bdd @specification
Feature: The models are the truth
  Pydantic models are the only description of the database. They say which tables
  exist, which columns each has, and what each column means. The library never asks
  the database what is there, because the models already said.

  Section "The models are the truth" of HOW-IT-SHOULD-WORK.md.

  Background:
    Given a model Person declaring the table "people" with columns id and name

  Scenario: the table name comes from the declaration
    When the library is asked which table Person names
    Then it answers "people" without opening a database

  Scenario: the columns come from the declaration
    When the library is asked which columns Person declares
    Then it answers id and name, and nothing else
    And it never asks the database what columns exist

  Scenario: a column the model does not declare is refused
    When a caller asks for people narrowed by "nickname"
    Then the library refuses in the caller's own process
    And the refusal names Person
    And the refusal lists id and name
    And no statement was built

  Scenario: the refusal never comes from the engine
    When a caller asks for people narrowed by "nickname"
    Then no engine was asked anything
    And the words "no such column" appear nowhere in the refusal

  Scenario: field declares meaning, not spelling
    Given a model Order declaring a column total that holds money with a floor of zero
    When the library renders that column for postgresql
    Then it declares a numeric type with the floor attached

  Scenario: the same meaning is spelled differently on another engine
    Given a model Order declaring a column total that holds money with a floor of zero
    When the library renders that column for sqlite
    Then it declares the engine's own numeric type with the floor attached
    And the model itself names no engine type
