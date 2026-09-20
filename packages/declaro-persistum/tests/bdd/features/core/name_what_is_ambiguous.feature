@bdd @specification
Feature: name_what_is_ambiguous -- A column gone and another appeared
  It is either a rename or a drop and an add, and the difference is your data.
  The new model says which: the column that replaced the old one names what it
  replaced.

  Scenario: a declared rename is a rename
    Given a column "email_address" declaring that it replaced "email"
    Then one rename comes back, not a drop and an add

  Scenario: nothing declared stops the migration
    Given "email" gone and "email_address" appeared, with neither naming the other
    Then it is refused
    And the fault names both columns and the declaration to write

  Scenario: an unrelated add is not ambiguous
    Given a new column and no column gone
    Then nothing is refused
