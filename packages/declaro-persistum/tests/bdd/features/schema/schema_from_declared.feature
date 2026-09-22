@bdd @specification
Feature: schema_from_declared -- The target schema, derived from the declarations
  The models are the desired state. Everything else is derived from them, and the
  database is never asked what belongs.

  Scenario: a model becomes a table
    Given a model declaring "people" with id and name
    When the target schema is built
    Then it holds a table "people" with those two columns

  Scenario: a declared view is in the target too
    Given a view declared against that model
    When the target schema is built
    Then the view is in it

  Scenario: nothing in the live database reaches the target
    Given a database holding a table no model declares
    When the target schema is built
    Then the target does not mention it

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/an_index_can_span_columns.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # An index is a promise about speed. `field` declares one column at a time, so a
  # promise about two columns together has nowhere to live, and a composite index is the
  # common case rather than an exotic one.

  Scenario: an index over one column is declared on the column
    Given a model declaring a column with an index
    When the database is opened
    Then the database holds that index

  Scenario: an index over two columns is declared on the model
    Given a model declaring an index over status and created_at
    When the database is opened
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
    When the database is opened
    Then the engine refuses a second row with the same pair

  Scenario: removing an index from the model removes it from the database
    Given a declared index already in the database
    When the model no longer declares it
    Then bringing the schema up to date drops it
