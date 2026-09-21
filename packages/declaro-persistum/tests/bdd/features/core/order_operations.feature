@bdd @specification
Feature: order_operations -- The order dependencies allow
  A table before the index on it, a parent before the child that points at it.

  Scenario: a table comes before its index
    Given a create for a table and a create for an index on it
    Then the table runs first

  Scenario: a parent comes before its child
    Given two tables where one references the other
    Then the referenced one runs first

  Scenario: two tables that reference each other are refused
    Given a cycle between two tables
    Then a fault comes back naming both

  # ------------------------------------------------------------------
  # ADDED 2026-09-21 WHILE THIS WAS BEING BUILT. The three scenarios above ask
  # only about things arriving. Every one of them runs at a deploy beside
  # things going away, and going away reverses the order: a table nothing
  # points at any more can be dropped, and a table something still points at
  # cannot. Without this scenario, an ordering that walked creates correctly
  # and left drops in the order they were written would pass, and the failure
  # would land halfway through a migration.

  Scenario: a child is dropped before the parent it points at
    Given a drop for a table and a drop for the table pointing at it
    Then the pointing one runs first
