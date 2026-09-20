@bdd @specification
Feature: difference -- What turns the live schema into the target
  Set theory over two schemas: what is only in the target is created, what is only
  in the live database is dropped, what differs is altered.

  Scenario: a table only in the target is created
    Given a live schema with no tables and a target with "people"
    Then there is a create for "people"

  Scenario: a table only in the live schema is dropped
    Given a live schema with "legacy" and a target without it
    Then there is a drop for "legacy"

  Scenario: a column that differs is altered
    Given a column declared as text and held as an integer
    Then there is an alter for that column

  Scenario: no difference is an answer
    Given two schemas that match
    Then nothing comes back, and that is distinguishable from a failure
