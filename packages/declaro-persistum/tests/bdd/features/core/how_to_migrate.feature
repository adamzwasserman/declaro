@bdd @specification
Feature: how_to_migrate -- How far back a failed migration rolls
  An engine either takes a whole migration or takes it one operation at a time,
  and that decides what a half-finished migration leaves behind. A caller who
  believed the wrong one would retry from the wrong place.

  Scenario: SQLite takes the whole migration or none of it
    Given sqlite
    Then a failure rolls the whole migration back

  Scenario: PostgreSQL takes the whole migration or none of it
    Given postgresql
    Then a failure rolls the whole migration back

  Scenario: Turso lands each operation on its own
    Given turso
    Then a failure leaves the operations before it standing

  Scenario: every engine answers
    When every engine the package names is asked
    Then each one answers with one of the two words and nothing else
