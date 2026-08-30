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

  Scenario: the library opens and closes the connection
    Given a sqlite database holding alice, bob and carol
    When a caller asks for people named "bob"
    Then a connection was opened and closed around the question
    And the caller never held it

  Scenario: several writes declared together travel as one transaction
    Given a sqlite database holding no people
    When a caller declares two people together and the second cannot be written
    Then the database holds no people
    And the caller is told the whole declaration failed

  Scenario: a dialect the library does not speak is refused by name
    When a database is opened for an engine the library does not speak
    Then it refuses and lists the engines it does speak
