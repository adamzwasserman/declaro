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

  # REWRITTEN 2026-10-10, and the old scenario was wrong about the engine. It
  # said Turso lands each operation on its own, on the reasoning that Turso will
  # not wrap a migration in a transaction. Measured against pyturso that day:
  # plain `BEGIN` rolls back a created table and an added column, and a
  # three-statement batch whose middle statement fails leaves the database
  # holding only what it started with. A caller who believed the old answer
  # would retry from the wrong place, which is what this feature opens by saying.
  Scenario: Turso takes the whole migration or none of it
    Given turso
    Then a failure rolls the whole migration back

  # A SCHEMA CHANGE OPENS WITH `BEGIN` AND NOT `BEGIN CONCURRENT`. Turso refuses
  # DDL inside a concurrent transaction in its own words: "DDL statements require
  # an exclusive transaction". `fn how_to_write` hands the crew `BEGIN
  # CONCURRENT` for data, and a migration is not data, which is why
  # `fn query_run_together` opens a migration group with the plain word.
  Scenario: a schema change does not run in a concurrent transaction
    Given turso
    Then the word a migration group opens with is BEGIN

  Scenario: every engine answers
    When every engine the package names is asked
    Then each one answers with one of the two words and nothing else
