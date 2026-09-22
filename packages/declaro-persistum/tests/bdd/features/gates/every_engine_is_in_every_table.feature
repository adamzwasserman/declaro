@bdd @specification
Feature: Every engine is in every table
  What differs between engines lives in dispatch tables, one row per engine, read by
  subscript so an engine nobody declared raises at the lookup. That only holds if the
  tables agree on which engines exist.

  ADDING AN ENGINE MEANS ADDING A ROW TO EACH, and nothing checks that you did. A row
  added to four tables and missed in three gives a `KeyError` in production on the
  path nobody exercised.

  A dispatch table keyed by engine is one whose keys are the three words
  `set engines` names in `docs/design/persistum.hd`. Which modules hold them is
  the package's own business, and the gate finds them rather than being told.

  Scenario: every table names the same engines
    When every dispatch table keyed by engine is read
    Then all of them carry the same set of keys

  Scenario: a table missing an engine is named
    Given a table carrying postgresql and sqlite but not turso
    When the gate runs
    Then it fails and names the table and the missing engine

  Scenario: an engine appearing in one table only is named
    Given a table carrying an engine no other table knows
    When the gate runs
    Then it fails and names both

  Scenario: the gate finds the tables itself
    When the gate looks for dispatch tables
    Then it finds them by shape rather than from a list somebody maintains

  Scenario: a gate that found no table fails rather than passing
    When the gate runs and finds no dispatch table at all
    Then it fails, because a gate that read nothing agrees with everything

  Scenario: a lookup with a fallback is refused
    Given a dispatch table read with `.get(key, something)`
    When the gate runs
    Then it fails, because a default re-opens the space the table closed
