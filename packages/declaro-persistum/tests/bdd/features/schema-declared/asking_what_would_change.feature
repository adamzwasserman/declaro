@bdd @specification
Feature: Asking what would change, without changing it
  "The library reads the live database, compares it to your models, and applies it"
  implies you can ask what the difference is and get an answer rather than an effect.
  Nobody deploys a schema change without being able to read it first.

  Scenario: the difference comes back as a value
    Given a model that has gained a column since the database was last brought up to date
    When a caller asks what would change
    Then it is given the operations, in the order they would run
    And the database is unchanged

  Scenario: no difference is an answer, not an empty effect
    Given a database already matching the models
    When a caller asks what would change
    Then it is given nothing to do
    And that is distinguishable from a question that failed

  Scenario: the answer reads without knowing SQL
    When a caller asks what would change
    Then each operation says what it does to which table and column
    And reading it needs no SQL

  Scenario: asking twice gives the same answer
    When a caller asks what would change, twice
    Then both answers are equal
    And neither changed anything

  Scenario: the answer names anything the library could not decide alone
    Given a column gone and another appeared
    When a caller asks what would change
    Then the ambiguity is in the answer beside the operations
