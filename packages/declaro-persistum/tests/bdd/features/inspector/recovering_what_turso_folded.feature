@bdd
Feature: Putting back what a column declared but the engine did not store
  As the differ, comparing what is declared against what is there
  I want a column carrying its reference and its CHECK
  So that a constraint the engine keeps only in the DDL is not read as absent

  # TURSO FOLDS SOME NAMES TO LOWER CASE AND KEEPS OTHERS. What a pragma hands
  # back is not always what was written, and a reference or a CHECK read only
  # from the original DDL has to be put back onto the column the pragma gave.
  #
  # Two functions did that, each adding one key. The key was the only thing
  # that differed.

  @precommit
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
