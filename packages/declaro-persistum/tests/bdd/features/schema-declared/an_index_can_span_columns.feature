@bdd @specification
Feature: An index is a declared promise, over one column or several
  An index is a promise about speed. `field` declares one column at a time, so a
  promise about two columns together has nowhere to live, and a composite index is the
  common case rather than an exotic one.

  Scenario: an index over one column is declared on the column
    Given a model declaring a column with an index
    When the schema is brought to the declared state
    Then the database holds that index

  Scenario: an index over two columns is declared on the model
    Given a model declaring an index over status and created_at
    When the schema is brought to the declared state
    Then the database holds one index over both, in that order

  Scenario: the order of the columns is kept
    Given an index declared over status then created_at
    Then the index is not the same as one over created_at then status

  Scenario: an index naming a column the model does not declare is refused
    Given an index declared over a column that is not there
    When it is declared
    Then it is refused, and the refusal names the model

  Scenario: a unique index is a promise as well as a constraint
    Given an index declared unique over two columns
    When the schema is brought to the declared state
    Then the engine refuses a second row with the same pair

  Scenario: removing an index from the model removes it from the database
    Given a declared index already in the database
    When the model no longer declares it
    Then bringing the schema up to date drops it
