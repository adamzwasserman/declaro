@bdd @specification
Feature: how_to_inspect -- The five questions an engine answers about itself
  Reading a live schema back means asking a database what it holds, and each
  engine is asked differently. Those differences live in a table, so the reader
  takes five questions and a connection and names no engine.

  Scenario: an engine gives five questions
    Given any engine
    Then the answer carries a question about its tables, its columns, its
    indexes, its rules and its views

  Scenario: no two engines ask the same way
    When every engine the package names is asked
    Then no two of them give the same five questions

  Scenario: every question is a statement, values kept out of the text
    When every engine the package names is asked
    Then each question carries its text and its values apart
