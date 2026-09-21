@bdd @specification
Feature: spell_view -- Writing the SELECT a declared view stands for
  A view holds a declaration, never a string, so the library writes the SELECT
  with the same compiler the data half uses.

  Scenario: a declared view becomes a SELECT
    Given a view declared as the orders whose status is "paid"
    When it is spelled for sqlite
    Then the statement selects from orders where status is bound to "paid"

  Scenario: the value is bound, not pasted
    Then "paid" came back beside the statement

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/a_view_is_a_named_declaration.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # You declare a view against your models and the library writes the SELECT. That
  # sentence only means something if a view holds a declaration rather than a string, which
  # is the same object an engine produces.

  Scenario: a view is declared against models
    Given a model Order declaring orders with columns id, status and total
    When a view "paid_orders" is declared as the orders whose status is "paid"
    Then the declaration contains no SELECT

  Scenario: the library writes the statement
    Given a declared view "paid_orders"
    When the schema is brought to the declared state
    Then the database holds a view, and the library wrote its SELECT

  Scenario: a view naming a column no model declares is refused
    Given a view declared against a column Order does not have
    When it is declared
    Then it is refused, and the refusal names Order

  Scenario: a view over two models works the relationship out
    Given Order references Person
    When a view is declared as orders with the name of the person who placed them
    Then the library works out the join from the declared reference

  Scenario: changing a view's declaration changes the database
    Given a declared view already in the database
    When its declaration changes
    Then bringing the schema up to date replaces the view
    And it does not report success while changing nothing

  Scenario: a view is read like a table
    Given a declared view "paid_orders"
    When a caller asks for its rows
    Then it answers the same way a model does
