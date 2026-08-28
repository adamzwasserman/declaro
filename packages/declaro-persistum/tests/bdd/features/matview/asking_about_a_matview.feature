@bdd
Feature: Asking the registry what it knows about a materialized view
  As code deciding whether a table is a view this package maintains
  I want to ask the registry rather than guess from the name
  So that a real table called `sales_summary` is not mistaken for a view

  # SQLite HAS NO MATERIALIZED VIEWS, so this package emulates one with a real
  # table plus a row in a registry. Three questions get asked of that registry
  # and each had its own function returning fixed SQL. The SQL was the only
  # thing that differed.
  #
  # Every scenario runs the SQL against a REAL SQLite database holding a real
  # registry, because a query that names a column the table does not have is
  # exactly the failure worth catching, and reading the string cannot see it.

  @precommit @sqlite
  Scenario: the is-it-one question finds a view that was registered
    Given a registry holding a view "sales_summary"
    When the is-it-one query is run for "sales_summary"
    Then it answers yes

  @precommit @sqlite
  Scenario: the is-it-one question does not find a table nobody registered
    Given a registry holding a view "sales_summary"
    When the is-it-one query is run for "orders"
    Then it answers no

  @precommit @sqlite
  Scenario: the metadata question gives back what was registered
    Given a registry holding a view "sales_summary"
    When the metadata query is run for "sales_summary"
    Then it gives back the name, the query and how it refreshes

  @precommit @sqlite
  Scenario: the list question names every view in the registry
    Given a registry holding views "a_view" and "b_view"
    When the list query is run
    Then it names both, in order
