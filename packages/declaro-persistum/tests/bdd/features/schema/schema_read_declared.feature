@bdd @specification
Feature: schema_read_declared -- Asking the code what it declares
  The models and the views a caller wrote, read rather than passed in. No door
  takes them: a caller that had to hand them over could hand a different set to
  each door and nothing would notice.

  Scenario: models and views come back together
    Given a project declaring two models and one view
    When the declarations are read
    Then both models and the view come back in one answer

  Scenario: what the code means to lose comes with them
    Given a project declaring that the table "legacy" is to be dropped
    When the declarations are read
    Then that permission is part of the answer

  Scenario: a model that cannot be read is a fault
    Given a models file that does not parse
    When the declarations are read
    Then a fault comes back naming the file

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/every_column_type_is_declarable.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # "The library never asks the database what is there" only holds if the loader
  # understands every type a model can write. A type it cannot read is a model that
  # cannot be declared, and the set it accepts is not written down anywhere, so nobody
  # can tell a gap from a decision.

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
