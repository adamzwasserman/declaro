@bdd
Feature: Asking SQLite what a table is made of
  As the differ, deciding what a migration must do
  I want the table's columns, indexes and keys read back
  So that what I compare against the schema is what the engine actually has

  # FOUR READERS, ONE SHAPE. Each names a pragma, quotes the identifier it is
  # given and hands the rows back. The pragma is the only thing that differs.
  #
  # THE QUOTING IS THE PART THAT BITES. A pragma takes a quoted identifier and
  # a double-quote inside a name has to be doubled, or half the names come
  # back broken and the differ rebuilds a table on every deploy.
  #
  # These run against a REAL SQLite database, because a pragma is the engine
  # answering and there is nothing to stand in for it.

  @precommit @sqlite
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
