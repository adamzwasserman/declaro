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
