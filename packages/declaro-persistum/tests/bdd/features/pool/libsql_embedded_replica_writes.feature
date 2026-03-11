@bdd
Feature: LibSQL embedded replica write visibility
  As a developer using LibSQL embedded replica mode
  I want writes to be visible after commit() + sync()
  So that reads on the same or new connections see committed data

  In embedded replica mode the local SQLite file is read-only.
  Writes are forwarded to Turso Cloud primary; sync() pulls
  changes back into the local replica.  After commit() + sync()
  any subsequent read — on the same connection or a fresh one —
  must see the inserted row.

  Background:
    Given a LibSQL embedded replica pool connected to Turso Cloud
    And the "items" table exists with columns:
      | column | type    | primary_key |
      | id     | TEXT    | true        |
      | name   | TEXT    | false       |
      | value  | INTEGER | false       |
    And the table is empty

  # ===========================================================================
  # Core write-visibility contract
  # ===========================================================================

  @libsql @precommit
  Scenario: Inserted row is visible on the same connection after commit and sync
    When I insert a row with id "row-001" name "alpha" value 1
    And I call commit() on the connection
    And I call sync() on the connection
    Then SELECT COUNT(*) on the same connection returns 1

  @libsql @precommit
  Scenario: Inserted row is visible on a new connection after commit and sync
    When I insert a row with id "row-001" name "alpha" value 1
    And I call commit() on the connection
    And I call sync() on the connection
    And I open a new connection to the same replica
    Then SELECT COUNT(*) on the new connection returns 1

  @libsql @precommit
  Scenario: Multiple inserts accumulate correctly after commit and sync
    When I insert a row with id "row-001" name "alpha" value 1
    And I call commit() on the connection
    And I call sync() on the connection
    And I insert a row with id "row-002" name "beta" value 2
    And I call commit() on the connection
    And I call sync() on the connection
    Then SELECT COUNT(*) on the same connection returns 2

  # ===========================================================================
  # Regression: count must not be stale after commit+sync
  # ===========================================================================

  @libsql @precommit
  Scenario: COUNT does not return stale value after commit and sync
    Given the table contains 3 rows
    When I insert a row with id "row-new" name "new" value 99
    And I call commit() on the connection
    And I call sync() on the connection
    Then SELECT COUNT(*) on the same connection returns 4
    And SELECT COUNT(*) on a fresh connection returns 4

  # ===========================================================================
  # Write-forwarding investigation: does the write reach Turso Cloud?
  # ===========================================================================

  @libsql
  Scenario: Write via embedded replica appears on a remote-only connection
    """
    If this scenario fails but the core scenarios pass it means sync() works
    but the write never reached Turso Cloud — the INSERT ran against a local
    WAL journal that was discarded or overwritten on sync().

    If this fails AND the core scenarios fail, writes are not being forwarded
    at all — the pool may need to use a separate remote connection for writes.
    """
    When I insert a row with id "row-fwd" name "forwarded" value 7
    And I call commit() on the connection
    And I call sync() on the connection
    And I open a remote-only connection to the Turso Cloud primary
    Then SELECT COUNT(*) on the remote connection returns 1

  # ===========================================================================
  # Connection.commit() is not a no-op
  # ===========================================================================

  @libsql @precommit
  Scenario: commit() advances the write to the primary — sync() alone is not enough
    """
    Verifies that commit() actually flushes the write.
    If sync() without commit() makes the row visible, it means the driver
    auto-commits and our explicit commit() call is harmless but redundant.
    If the row is only visible after commit() + sync(), our current design is correct.
    """
    When I insert a row with id "row-c1" name "gamma" value 3
    And I call sync() WITHOUT calling commit() first
    Then SELECT COUNT(*) on a fresh connection returns 0

  @libsql @precommit
  Scenario: commit() then sync() makes the row visible — sync() alone does not
    When I insert a row with id "row-c2" name "delta" value 4
    And I call commit() on the connection
    And I call sync() on the connection
    Then SELECT COUNT(*) on a fresh connection returns 1

  # ===========================================================================
  # Pool-level behaviour: acquire() → write → release → acquire() → read
  # ===========================================================================

  @libsql @precommit
  Scenario: Write on one pooled connection is visible on a subsequently acquired connection
    When I acquire a connection from the pool and insert a row with id "row-p1" name "epsilon" value 5 then release it
    And I acquire a new connection from the pool
    Then SELECT COUNT(*) on the new connection returns 1
