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

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/a_name_the_library_owns_is_refused.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # The library keeps its own tables and columns beside yours. A model that declares one
  # of those names collides with it, and the collision surfaces as a migration that
  # destroys something or a query that reads the wrong rows.
  # The names it owns start `_declaro_meta`, `_dp_`, `_declaro_tmp_`, and `sqlite_`
  # and `turso_` belong to the engines.

  Scenario: a table named after one the library owns is refused
    Given a model declaring the table "_declaro_meta"
    When it is declared
    Then it is refused, and the refusal says the library owns that name

  Scenario: a table whose name starts with a reserved prefix is refused
    Given a model declaring the table "_dp_orders"
    When it is declared
    Then it is refused

  Scenario: a table named after an engine's own is refused
    Given a model declaring the table "sqlite_master"
    When it is declared
    Then it is refused

  Scenario: an ordinary name that merely contains a prefix is accepted
    Given a model declaring the table "shop_dp_orders"
    When it is declared
    Then it is accepted, because the prefix is not at the start

  Scenario: the reserved names are one table, not a pattern in the code
    When the reserved names are read
    Then they are rows carrying the name and who owns it
