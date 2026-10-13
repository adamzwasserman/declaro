@bdd @specification
Feature: how_to_spell -- How one engine writes one schema change
  Thirteen kinds of change and three engines. What each engine writes lives in a
  table, so the speller reads an answer rather than knowing an engine.

  Scenario: creating a table is one statement on every engine
    Given create_table on any engine
    Then the answer is one statement and says nothing is rebuilt

  Scenario: SQLite alters a column by rebuilding the table
    Given alter_column and sqlite
    Then the answer says the table is rebuilt

  Scenario: Turso rebuilds too
    Given alter_column and turso
    Then the answer says the table is rebuilt

  Scenario: PostgreSQL alters a column in place
    Given alter_column and postgresql
    Then the answer says nothing is rebuilt

  # A REBUILD IS FOUR STATEMENTS AND THE ORDER IS THE WHOLE OF IT. Make the new
  # table, copy what survives, drop the old one, put the new one under the old
  # name. Any other order loses the data.
  Scenario: a rebuild says what it takes, in order
    Given alter_column and sqlite
    Then the answer carries 4 statements, and the copy comes before the drop

  Scenario: every kind of change answers on every engine
    When all thirteen kinds are asked of all three engines
    Then every pair answers

  Scenario: a kind of change nobody wrote down is a fault, not a guess
    Given a kind of change the table does not cover
    Then a fault comes back naming it and the engine

  # ------------------------------------------------------------------
  # WHETHER THERE IS A PRIMARY IS THE THIRD THING THIS FUNCTION IS ASKED, added
  # 2026-10-12. A statement that runs on the local copy is not a statement that
  # reaches the primary, so the engine's name does not settle what to write.
  #
  # MEASURED 2026-10-10 AGAINST A REAL TURSO PRIMARY, one change per table, read
  # back on a replica that had never seen it. A push carries create table, add
  # column, drop column and ordinary rows. It carries neither ALTER TABLE RENAME
  # COLUMN nor ALTER TABLE RENAME TO: each runs locally, reports success, and
  # sends nothing, so the copy and the primary part company with no error
  # anywhere.
  #
  # AND THE REBUILD IS WHY THIS IS NOT TWO BROKEN CHANGES OUT OF THIRTEEN.
  # A rebuild ends with ALTER TABLE RENAME TO, and SQLite and Turso rebuild for
  # an alter and for a drop, so the rename sits underneath changes that never
  # mention one. Measured 2026-10-12: the push after a rebuild failed outright
  # with a parse error, the primary was left holding the temporary table and had
  # lost the real one, and the next push on that channel failed as well. The
  # local copy was correct the whole time.

  Scenario: a rename on a copy with no primary is still one statement
    Given rename_column and turso with no primary
    Then the answer carries 1 statement, and nothing is committed between them

  Scenario: a renamed column on a replicated database is added, copied and dropped
    Given rename_column and turso with a primary
    Then the answer carries 3 statements, and the copy comes before the drop
    And no statement renames anything
    And each statement is committed on its own

  Scenario: a renamed table on a replicated database is created, copied and dropped
    Given rename_table and turso with a primary
    Then the answer carries 3 statements, and the copy comes before the drop
    And no statement renames anything
    And each statement is committed on its own

  # A DROP REPLICATES ON ITS OWN AND A REBUILD DOES NOT, so the replicated
  # spelling is the plain statement that every other engine writes.
  Scenario: a column dropped on a replicated database is not rebuilt around
    Given drop_column and turso with a primary
    Then the answer says nothing is rebuilt
    And no statement renames anything

  # A COLUMN CANNOT BE ALTERED IN PLACE ON TURSO AND A REBUILD CANNOT REPLICATE,
  # so there is nothing honest left to write and this says so.
  Scenario: altering a column on a replicated database is refused
    Given alter_column and turso with a primary
    Then a fault comes back saying the change cannot reach the primary

  Scenario: a database with no primary never asks for a commit between statements
    When all thirteen kinds are asked of all three engines
    Then no pair says its statements are committed on their own
