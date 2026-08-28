@bdd
Feature: Adding a value to an enum, and taking one away
  As a schema author widening a Literal type
  I want the new value to reach the lookup table the column points at
  So that the column accepts it instead of refusing every row that uses it

  # A `Literal["a", "b"]` becomes a lookup table plus a foreign key, because a
  # CHECK constraint is not portable. Widening the Literal means inserting a
  # row; narrowing it means deleting one.
  #
  # BOTH RUN AGAINST A REAL DATABASE with real foreign key enforcement, because
  # the interesting half is what happens when a row still points at the value
  # being removed. Reading the SQL string cannot see that.

  @precommit @sqlite
  Scenario: a value added to the lookup is a value the column accepts
    Given a table "orders" whose status is one of "new" and "paid"
    When "shipped" is added to the statuses
    Then a row with status "shipped" is accepted

  @precommit @sqlite
  Scenario: a value the column never had is refused before it is added
    Given a table "orders" whose status is one of "new" and "paid"
    Then a row with status "shipped" is refused

  @precommit @sqlite
  Scenario: a value taken away is a value the column stops accepting
    Given a table "orders" whose status is one of "new" and "paid"
    When "paid" is taken out of the statuses
    Then a row with status "paid" is refused

  @precommit @sqlite
  Scenario: a value still in use cannot be taken away
    Given a table "orders" whose status is one of "new" and "paid"
    And an order whose status is "paid"
    When "paid" is taken out of the statuses
    Then the engine refuses, because a row still points at it

  @precommit
  Scenario Outline: a quote in a value does not end the statement early
    When "<value>" is <change> the statuses
    Then the statement carries the value with its quote doubled

    Examples:
      | value    | change        |
      | it's new | added to      |
      | it's new | taken out of  |
