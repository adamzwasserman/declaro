@bdd @specification
Feature: replicates -- Whether this database replicates at all
  Derived from the primary rather than stored beside it, because a second flag
  could disagree with the first. It decides whether any sync card is reached.

  Scenario: a database with a primary replicates
    Given a database opened against a primary
    Then it replicates

  Scenario: a local database does not
    Given a database opened with no primary
    Then it does not replicate

  Scenario: nothing else records the answer
    Then no field anywhere says it, beside the primary itself
