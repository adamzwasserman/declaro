@bdd @specification
Feature: mechanism_for -- Picking what a caller refused to name
  A caller says how fresh a materialized view must be. PostgreSQL has a real
  materialized view and SQLite does not, and bridging that is the library's job
  rather than the caller's.

  Scenario: PostgreSQL uses what it has
    Given a view declared "current when read" on postgresql
    Then a materialized view is used

  Scenario: SQLite is built out of what it has
    Given the same declaration on sqlite
    Then a table, its triggers and a registry row are used

  Scenario: the caller never names a mechanism
    Then no declaration anywhere says on_commit or trigger
