@bdd @specification
Feature: Every engine is in every table
  What differs between engines lives in dispatch tables, one row per engine, read by
  subscript so an engine nobody declared raises at the lookup. That only holds if the
  tables agree on which engines exist.

  ADDING AN ENGINE MEANS ADDING A ROW TO EACH, and nothing checks that you did. A row
  added to four tables and missed in three gives a `KeyError` in production on the
  path nobody exercised.

  The tables are `INSPECTORS`, `APPLIERS`, `SQL_GENERATORS`, `WRITERS`,
  `TRANSACTIONS`, `FACTS` and `OPERATORS`.

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

  Scenario: the gate counts the tables it found
    When the gate runs
    Then it reports how many tables it read
    And a run that found fewer than the known count fails

  Scenario: a lookup with a fallback is refused
    Given a dispatch table read with `.get(key, something)`
    When the gate runs
    Then it fails, because a default re-opens the space the table closed
