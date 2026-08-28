@bdd
Feature: Putting a page limit and an offset on a query
  As code paging through results
  I want to set a limit and an offset, and set them again
  So that a second call does not leave two LIMIT clauses on one statement

  # ADD OR REPLACE, and the replace half is the reason these are not string
  # concatenation. Calling it twice must leave one clause, not two.
  #
  # The SQL is RUN against a real database, because "SELECT ... LIMIT 2 LIMIT 1"
  # is a plausible string and only the engine says it is not a statement.

  @precommit @sqlite
  Scenario Outline: a page size is put on a query that had none
    Given five rows in a table
    When the query is given a <clause> of <value>
    Then the statement runs and returns <rows> rows

    Examples:
      | clause | value | rows |
      | limit  | 2     | 2    |
      | offset | 3     | 2    |

  @precommit @sqlite
  Scenario Outline: setting it twice replaces rather than repeats
    Given five rows in a table
    When the query is given a <clause> of <first> and then <second>
    Then the statement runs and returns <rows> rows
    And the statement carries one <clause> clause, not two

    Examples:
      | clause | first | second | rows |
      | limit  | 4     | 1      | 1    |
      | offset | 1     | 4      | 1    |

  @precommit
  Scenario Outline: the parameters and the engine travel with the query
    When the query is given a <clause> of 1
    Then its parameters and its engine are unchanged

    Examples:
      | clause |
      | limit  |
      | offset |
