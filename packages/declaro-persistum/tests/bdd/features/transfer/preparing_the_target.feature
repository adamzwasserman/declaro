@bdd
Feature: Turning off the target's foreign key checks before a transfer
  As an operator copying tables that reference each other
  I want the run to stop if the target will not drop its checks
  So that I am not handed a report naming healthy tables

  # Two tables that reference each other cannot be loaded in any order with
  # foreign key checks on. So the transfer turns them off for the run. If that
  # fails and the run continues, every table in the cycle fails on its first
  # row, each failure is recorded against that table, and the operator reads
  # "2 tables failed" about two tables that are fine.
  #
  # `target_stop_checks` and `target_restore_checks` are the two doors in
  # `docs/design/persistum-transfer.hd` that this feature is about. Each hands
  # back nothing or a fault, so every scenario below reads a return value.

  @precommit
  Scenario: the target's checks are off for the run
    Given a target that drops its checks
    When the checks are turned off for two tables that reference each other
    Then nothing came back

  @precommit
  Scenario: the run stops when the target refuses to drop its checks
    Given a target that refuses to drop its checks
    When the checks are turned off for two tables that reference each other
    Then it refuses
    And it names both tables
    And it says transferring now would blame those tables

  @precommit
  Scenario: the checks go back on when the run is over
    Given a target that restores its checks
    When the checks are turned back on
    Then nothing came back

  @precommit
  Scenario: a target that will not put its checks back says so
    Given a target that refuses to restore its checks
    When the checks are turned back on
    Then the refusal came back

  # WHERE A FAILED RESTORE ENDS UP. The rows did transfer, so every table is
  # still counted as copied and no `TableResult` carries a failure. What the
  # operator has to be told is that the target is left unenforced, and
  # `transfer_run` says so the only way it can: by handing back a `Fault`.

  @precommit
  Scenario: a failed restore reaches the operator even though every table copied
    Given a target that refuses to restore its checks
    When every table copies and the checks are put back
    Then the refusal came back

  @precommit
  Scenario: the refusal names the target and what the target said
    Given a target that refuses to restore its checks
    When every table copies and the checks are put back
    Then the refusal names the target as left unchecked
    And it says what the target said

  @precommit
  Scenario: a run whose checks went back on blames no table
    Given a target that restores its checks
    When every table copies and the checks are put back
    Then every table is counted as copied
    And no table carries a failure
