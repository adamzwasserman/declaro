@bdd @specification
Feature: refuse_owned_names -- A name the library or an engine owns
  The library keeps its own tables and columns beside yours, and a collision
  surfaces as a migration that destroys something or a query that reads the wrong
  rows. The prefixes are `_declaro_meta`, `_dp_`, `_declaro_tmp_`, and `sqlite_`
  and `turso_`, which belong to the engines.

  Scenario Outline: a model claiming an owned name is refused
    Given a model declaring the table <name>
    Then it is refused, naming the prefix

    Examples:
      | name             |
      | "_declaro_meta"  |
      | "_dp_enum_"      |
      | "sqlite_master"  |
      | "turso_internal" |

  Scenario: an ordinary name is accepted
    Given a model declaring the table "people"
    Then it is accepted
