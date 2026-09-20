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
