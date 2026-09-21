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

  # ------------------------------------------------------------------
  # ADDED 2026-09-21 WHILE THIS WAS BEING BUILT. The three scenarios above
  # left two questions open, and either answer to either one decides whether a
  # migration runs at all.
  #
  # A model that names a column in `dropping` has already said it means to lose
  # what was in that column. Without the scenario below, adding a column to a
  # table in the same change that drops one from it is refused however the
  # models are written, and there is nothing a reader could write to settle it.
  #
  # A rename moves a column inside one table. Pairing a column gone from one
  # table with a column that appeared on another would refuse a pair of changes
  # that have nothing to do with each other.

  Scenario: a drop the model asked for is not ambiguous
    Given "email" named in the model's dropping and "email_address" appeared
    Then nothing is refused

  Scenario: a column gone from one table and one appeared on another is not ambiguous
    Given "name" gone from "users" and "full_name" appeared on "orders"
    Then nothing is refused
