@bdd
Feature: Naming the database the example app should talk to
  As someone reading the example to learn the shape
  I want each engine described the same way
  So that adding a fifth is filling in a row rather than writing a function

  # FOUR BUILDERS, EACH RETURNING THE SAME RECORD. What differed was the
  # dialect, the connection string and what to call it on screen. Two of them
  # took a path and differed by nothing else at all.
  #
  # THE PATH IS REQUIRED. It used to default to a constant, so the function
  # could not tell "I chose the default" from "I forgot to pass one".

  @precommit
  Scenario Outline: each path-based engine describes itself
    When a config is built for "<engine>" at "/tmp/todos.db"
    Then its dialect is "<dialect>"
    And it is shown as "<shown>"
    And it carries no auth token

    Examples:
      | engine          | dialect         | shown                     |
      | sqlite          | sqlite          | SQLite (todos.db)         |
      | turso_embedded  | turso_embedded  | Turso Embedded (todos.db) |

  @precommit
  Scenario: PostgreSQL builds a URL from its five parts
    When a PostgreSQL config is built for "todos" on "db.example.com"
    Then its dialect is "postgresql"
    And its connection string names the host and the database

  @precommit
  Scenario: Turso Cloud carries the token it was given
    When a Turso Cloud config is built with the token "secret"
    Then its dialect is "turso_cloud"
    And it carries the token it was given

  @precommit
  Scenario: a path-based builder refuses to invent a path
    When a config is built for "sqlite" with no path at all
    Then it refuses
