@bdd
Feature: Whether a failed table is the engine's fault or ours
  As an operator reading "4 tables failed"
  I want that report to name tables that are actually broken
  So that I do not open a healthy database looking for a fault in it

  # Copying one table can fail two ways, and they need opposite handling.
  # The engine refusing one table is that table's problem: record it and copy
  # the other thirty-nine. A bug in this package fails on EVERY table, so
  # recording it per table turns one defect into forty, and the report names
  # forty tables with nothing wrong with any of them. Measured 2026-08-17:
  # one TypeError produced tables_failed=4 and all four were healthy.
  #
  # WHY THIS FILE EXISTS. Six tests made that failure happen by replacing the
  # copy step on the module. The copy step is reachable no other way, because
  # the run record does not carry it, so patching the module was the only
  # handle a test had.

  @precommit
  Scenario Outline: a bug of ours reaches the caller whole
    Given a run whose copy step raises the bug <ours>
    When one table is copied
    Then <ours> reaches the caller
    And no table is blamed

    Examples:
      | ours           |
      | TypeError      |
      | AttributeError |
      | KeyError       |
      | NameError      |
      | IndexError     |

  @precommit
  Scenario: a table the engine refuses is recorded against that table
    Given a run whose copy step raises a refusal from the engine
    When one table is copied
    Then the table is recorded as failed
    And the failure names the engine's own error

  @precommit
  Scenario: a table that copies is reported with its row count
    Given a run whose copy step moves 7 rows
    When one table is copied
    Then the table is recorded as transferred
    And it reports 7 rows

  @precommit
  Scenario: _record_a_failed_table blames one table and no other
    When _record_a_failed_table is given a refusal for "orders"
    Then the table is recorded as failed
    And it reports 0 rows
