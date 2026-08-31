@bdd @specification
Feature: Every type a model can declare, the loader understands
  "The library never asks the database what is there" only holds if the loader
  understands every type a model can write. A type it cannot read is a model that
  cannot be declared, and the set it accepts is not written down anywhere, so nobody
  can tell a gap from a decision.

  Scenario Outline: a declared type becomes a column
    Given a model declaring a column of type <declared>
    When the loader reads it
    Then the column carries the type <becomes>

    Examples:
      | declared        | becomes   |
      | str             | text      |
      | int             | integer   |
      | float           | real      |
      | bool            | boolean   |
      | bytes           | blob      |
      | Decimal         | numeric   |
      | date            | date      |
      | datetime        | timestamp |
      | time            | time      |
      | UUID            | uuid      |
      | list[str]       | jsonb     |
      | dict[str, str]  | jsonb     |
      | Literal["a","b"]| text      |

  Scenario: a nullable type is a column that may be absent
    Given a model declaring a column of type `str | None`
    When the loader reads it
    Then the column is nullable

  Scenario: a type the loader does not understand is refused by name
    Given a model declaring a column of type `tuple[int, int]`
    When the loader reads it
    Then it refuses, names the type, and lists the ones it understands

  Scenario: the set of accepted types is one table
    When the accepted types are read
    Then they are rows in one table
    And a test can enumerate them without reading the loader

  Scenario: every accepted type has a spelling on every engine
    When each accepted type is rendered for each engine
    Then every pair produces a type that engine holds
    And a pair with no spelling fails rather than falling back to text
