@bdd @specification
Feature: What the library owns
  The caller owns the description. The library writes the statement, picks the
  parameter style, chooses the connection, and absorbs every difference between
  PostgreSQL, SQLite and Turso.

  The test: if a decision could go differently on a different engine, it belongs to
  the library.

  Section "What the library owns" of HOW-IT-SHOULD-WORK.md.

  Background:
    Given a model Person declaring the table "people" with columns id and name

  Scenario Outline: the same declaration runs on every engine
    Given a <engine> database holding alice, bob and carol
    When a caller asks for people named "bob"
    Then it is given one person, and that person is bob

    Examples:
      | engine     |
      | sqlite     |
      | postgresql |
      | turso      |

  Scenario Outline: the caller writes the same code on every engine
    Given a <engine> database holding alice, bob and carol
    When a caller asks for people named "bob"
    Then the caller's code named no engine
    And the caller's code held no connection

    Examples:
      | engine     |
      | sqlite     |
      | postgresql |
      | turso      |

  Scenario: moving between engines changes one line
    Given an application reading and writing people on sqlite
    When the line that opens the database is changed to postgresql
    Then every other line is unchanged
    And the application behaves the same

  Scenario: the library picks the parameter style
    Given a postgresql database holding alice, bob and carol
    When a caller asks for people named "bob"
    Then the statement the engine received used that engine's own placeholders
    And the caller wrote no placeholder

  # A CONNECTION IS OPENED ONCE AND HELD FOR LIFE, NOT OPENED PER QUESTION.
  # Rewritten 2026-09-21: this used to ask that a connection be opened and closed
  # around the question, which is the arm that measured 250 writes a second
  # against 1,505. Every connection is an operating system thread, and reusing
  # one is six times faster on its own. The claim underneath was always about who
  # owns it rather than how often it opens.
  Scenario: the library owns the connection and the caller never touches it
    Given a sqlite database holding alice, bob and carol
    When a caller asks for people named "bob"
    Then the library chose the connection and opened it
    And the caller never held it, read it, or ran anything on it

  Scenario: the connection a question runs on was already open
    Given a sqlite database holding alice, bob and carol
    When a caller asks two questions one after the other
    Then the second did not open a connection of its own

  Scenario: several writes declared together travel as one transaction
    Given a sqlite database holding no people
    When a caller declares two people together and the second cannot be written
    Then the database holds no people
    And the caller is told the whole declaration failed

  Scenario: an engine the library does not speak is refused by name
    When a database is opened for an engine the library does not speak
    Then it refuses and lists the engines it does speak
