@bdd @specification
Feature: refuse_undeclared_columns -- Stopping a caller in their own process
  Ask for a column no model declares and the library names your model and lists
  the columns it does declare. You never see "no such column" from an engine,
  because the request never got that far.

  Scenario: an undeclared column is refused by name
    Given a model Person declaring id and name
    When a caller asks for "nmae"
    Then the refusal names Person and lists id and name

  Scenario: nothing reached the database
    Then no connection was used

  Scenario: every grammar refuses the same column
    Then the four grammars give the same refusal
