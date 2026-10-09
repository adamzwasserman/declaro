@bdd @specification
Feature: Every field a change carries is a field the writer uses
  `fn what_an_operation_carries` is a table saying which fields one kind of
  schema change fills. `fn spell_operation` turns that change into statements.
  Nothing checked that the two agree, so a field could be carried and never read,
  and a row could name the wrong fields entirely.


  WHY A TABLE AND A WRITER CAN DRIFT. Each is right on its own. The table reads
  like documentation and the writer reads like code, and a reader checking one
  has no reason to open the other. Only asking both the same question catches it,
  which is what these scenarios do.

  THE VALUE MAY BE BOUND RATHER THAN WRITTEN. A statement is text and the values
  kept beside it, so `insert_enum_values` spelling two values gives two
  statements with each value as a parameter and neither in the text. A check that
  reads only the text calls that a missing field. It is not one, and the first
  draft of this gate got it wrong that way.


  # WRITTEN 2026-10-07 after two live bugs, both found by measuring rather than
  # reading. A create_table carries indexes and spelling one hands back a single
  # CREATE TABLE, so the index is gone. That is the same bug another project hit
  # on persistum 0.1.11, where the differ packed a new table's indexes into the
  # operation and the applier never read them. It survived the rewrite.
  #
  # The second is worse because it is a wrong answer rather than a missing one.
  # `rename_column` claims it carries `columns` and claims it does not carry
  # `renamed_to`. Measured both ways: given `renamed_to` and no columns it writes
  # `ALTER TABLE "people" RENAME COLUMN "a" TO "b"`, and given columns and no
  # `renamed_to` it refuses with "handed to sqlite without to". The row is
  # backwards and both halves of it are wrong.
  # ASKED PER KIND AND NOT PER ENGINE. Whether a change needs a field is the
  # kind's business and whether it uses one is the engine's. `alter_column`
  # carries every index the table must end with, because SQLite and Turso alter a
  # column by building a new table and copying into it. PostgreSQL does it in one
  # statement and needs none of them, and demanding every engine read every
  # carried field would call the simpler spelling broken.

  Scenario: every field the table claims is used by some engine
    Given one change of every kind, with every field the table claims filled
    When each one is spelled for each engine
    Then every claimed field shows up in the statements or their values, on some engine

  Scenario: a field the table denies is not needed
    Given one change of every kind, carrying only the fields the table claims
    When each one is spelled for each engine
    Then none of them is refused for want of a field

  Scenario: a new table's indexes are spelled
    Given a create_table carrying two columns and one index
    When it is spelled for each engine
    Then one statement makes the table and another makes the index
    And the index is named in the statement that makes it

  Scenario: a renamed column needs the new name and not the old definitions
    Given a rename_column carrying the new name and no column definitions
    When it is spelled for each engine
    Then it is spelled, and the statement names both the old and the new name

  Scenario: the table and the writer name the same kinds
    When every kind in the declaration is read
    Then the table answers for each one
    And the writer answers for each one on each engine

  Scenario: the gate reads the kinds from the declaration, not from a list here
    When the gate looks for the kinds of change
    Then it reads `set operation_kinds` in docs/design/persistum.hd
    And a kind added there is checked without anybody editing the gate

  Scenario: a gate that spelled nothing fails rather than passing
    When the gate runs and spells no statement at all
    Then it fails, because a gate that read nothing agrees with everything
