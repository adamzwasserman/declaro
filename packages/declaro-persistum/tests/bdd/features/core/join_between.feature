@bdd @specification
Feature: join_between -- The join the foreign key already declared
  You declared the relationship once, in the model, and naming it again in every
  query is the duplication this library exists to remove.

  Scenario: the rows on the other side
    Given Order declaring a column that references Person
    When a caller asks for the orders of alice
    Then the join was worked out from that column

  Scenario: narrowing by something on the far side
    When a caller asks for orders where the person's name is "bob"
    Then the same column is what joined them

  Scenario: two models with no key between them
    When a caller asks across two unrelated models
    Then a fault comes back naming both and saying no key joins them
