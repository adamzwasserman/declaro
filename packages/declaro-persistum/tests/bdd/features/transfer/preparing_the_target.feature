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
  # WHY THIS FILE EXISTS. Three tests made that failure happen by swapping the
  # whole loader table on the module. The functions take the loader as an
  # argument, so nothing had to be swapped: the test can hand one in.

  @precommit
  Scenario: _turn_fk_checks_off says one connection had its checks dropped
    Given a loader whose target drops its checks
    When the checks are turned off for two tables that reference each other
    Then it answers 1 connection

  @precommit
  Scenario: _turn_fk_checks_off stops the run when the target refuses
    Given a loader whose target refuses to drop its checks
    When the checks are turned off for two tables that reference each other
    Then it refuses
    And it names both tables
    And it says transferring now would blame those tables

  @precommit
  Scenario: _leave_fk_checks_on says no connection was touched
    Given a loader whose target drops its checks
    When there is no cycle so nothing is turned off
    Then it answers 0 connections

  @precommit
  Scenario: _turn_fk_checks_back_on says nothing went wrong
    Given a loader whose target restores its checks
    When the checks are turned back on
    Then nothing came back

  @precommit
  Scenario: _turn_fk_checks_back_on hands back what stopped it
    Given a loader whose target refuses to restore its checks
    When the checks are turned back on
    Then the refusal came back

  @precommit
  Scenario: _leave_the_checks_as_they_are has nothing to hand back
    Given a loader whose target restores its checks
    When nothing was turned off so nothing is restored
    Then nothing came back

  # WHERE A FAILED RESTORE ENDS UP. The rows did transfer, so `success` stays
  # true about the rows, and the target left unenforced goes into `errors`,
  # which is the field that already exists for a failure the transfer survived.

  @precommit
  Scenario: _left_unenforced hands a failed restore back when nothing else failed
    When _left_unenforced is given a refusal and nothing else is going wrong
    Then the refusal came back

  @precommit
  Scenario: _left_unenforced has nothing to hand back when the restore worked
    When _left_unenforced is given no refusal
    Then nothing came back

  @precommit
  Scenario: the result names the unenforced target and what caused it
    When the result is built from a refusal
    Then it names the target as unchecked
    And it names what the target said

  @precommit
  Scenario: the result says nothing extra when the restore worked
    When the result is built from no refusal
    Then it adds nothing to the errors
