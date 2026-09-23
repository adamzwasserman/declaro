@bdd @specification
Feature: how_to_inspect -- The five questions an engine answers about itself
  Reading a live schema back means asking a database what it holds, and each
  engine is asked differently. Those differences live in a table, so the reader
  takes five questions and a connection and names no engine.

  It asks so the two can be compared, never so it can decide what belongs. The
  models say what should be there.

  Scenario: an engine gives five questions
    Given any engine
    Then the answer carries a question about its tables, its columns, its indexes, its rules and its views

  Scenario: every engine answers
    When every engine the package names is asked
    Then each one gives all five

  # SQLITE AND TURSO GIVE THE SAME FIVE, AND THAT IS A ROW RATHER THAN AN ALIAS.
  # Turso is a Rust rewrite aiming at SQLite compatibility, not a fork of it, so
  # the day its catalogue differs one row changes and nothing else does. What
  # would be wrong is all three being alike, because then the table says nothing.
  Scenario: the engines do not all ask the same way
    When every engine the package names is asked
    Then more than one set of five comes back

  Scenario: every question keeps its values out of its text
    When every engine the package names is asked
    Then each question carries its text and its values apart

  Scenario: a question about one table binds the table's name
    When every engine the package names is asked
    Then the questions asked per table bind the name rather than writing it in
