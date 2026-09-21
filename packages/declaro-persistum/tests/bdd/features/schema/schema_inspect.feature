@bdd @specification
Feature: schema_inspect -- Asking a database what it currently holds
  The only place this library asks a database what is there, and it asks so it
  can compare, never so it can decide what belongs. What belongs is in the
  models.

  Scenario: the live schema comes back
    Given a database holding one table with two columns
    When it is inspected
    Then the table and both columns come back

  Scenario: an empty database is an answer
    Given a database with no tables
    When it is inspected
    Then an empty schema comes back, and that is not a failure

  Scenario: nothing decides anything from it
    When a database holds a table no model declares
    Then the table is not adopted into the target schema

  # ------------------------------------------------------------------
  # FOLDED IN FROM `inspector/recovering_what_turso_folded.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As the differ, comparing what is declared against what is there
  # I want a column carrying its reference and its CHECK
  # So that a constraint the engine keeps only in the DDL is not read as absent
  # TURSO FOLDS SOME NAMES TO LOWER CASE AND KEEPS OTHERS. What a pragma hands
  # back is not always what was written, and a reference or a CHECK read only
  # from the original DDL has to be put back onto the column the pragma gave.
  #
  # Two functions did that, each adding one key. The key was the only thing
  # that differed.
  # @precommit

  Scenario: a column gains the reference the DDL declared
    Given a column read back from a pragma
    When the reference "people.id" is put back on it
    Then it carries reference "people.id"
    And everything the pragma said is still there

  @precommit
  Scenario: a column gains the CHECK the DDL declared
    Given a column read back from a pragma
    When the check "age > 0" is put back on it
    Then it carries check "age > 0"
    And everything the pragma said is still there

  @precommit
  Scenario: putting one back does not disturb the column it was given
    Given a column read back from a pragma
    When the reference "people.id" is put back on it
    Then the column it was given is unchanged

  # ------------------------------------------------------------------
  # FOLDED IN FROM `pragma/reading_a_pragma.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As the differ, deciding what a migration must do
  # I want the table's columns, indexes and keys read back
  # So that what I compare against the schema is what the engine actually has
  # FOUR READERS, ONE SHAPE. Each names a pragma, quotes the identifier it is
  # given and hands the rows back. The pragma is the only thing that differs.
  #
  # THE QUOTING IS THE PART THAT BITES. A pragma takes a quoted identifier and
  # a double-quote inside a name has to be doubled, or half the names come
  # back broken and the differ rebuilds a table on every deploy.
  #
  # These run against a REAL SQLite database, because a pragma is the engine
  # answering and there is nothing to stand in for it.
  # @precommit @sqlite

  Scenario: the column reader names every column the table has
    Given a table "people" with columns id and name
    When its columns are read back
    Then the answer names id and name

  @precommit @sqlite
  Scenario: the index reader names an index that was declared
    Given a table "people" with an index "by_name" over name
    When its indexes are read back
    Then the answer names "by_name"

  @precommit @sqlite
  Scenario: the index-column reader names what one index covers
    Given a table "people" with an index "by_name" over name
    When the columns of "by_name" are read back
    Then the answer names name

  @precommit @sqlite
  Scenario: the foreign-key reader names the table a key points at
    Given a table "orders" whose customer references "people"
    When its foreign keys are read back
    Then the answer names "people"

  @precommit @sqlite
  Scenario Outline: every reader survives a name with a quote in it
    Given a table named with a quote in it
    When "<reader>" is asked about it
    Then it answers without raising

    Examples:
      | reader                   |
      | pragma_table_info        |
      | pragma_index_list        |
      | pragma_foreign_key_list  |
