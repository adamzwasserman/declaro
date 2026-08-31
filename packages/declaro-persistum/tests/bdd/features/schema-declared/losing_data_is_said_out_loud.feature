@bdd @specification
Feature: A change that loses data says so
  Dropping a column loses what was in it. Dropping a table loses all of it. Narrowing
  a type loses what no longer fits. The library derives these from your models without
  being asked, so it has to say which of them destroy something.

  Scenario Outline: a destructive operation is named as destructive
    When the library works out that it would <operation>
    Then the answer says that operation loses data

    Examples:
      | operation                      |
      | drop a column                  |
      | drop a table                   |
      | narrow a column's type         |
      | drop a table other rows point at |

  Scenario: an additive operation is not named as destructive
    When the library works out that it would add a column
    Then the answer does not say it loses data

  Scenario: a destructive change is refused unless it is asked for
    Given a model change that would drop a column
    When the library is asked to bring the database up to date
    Then it refuses and says which operation loses what

  Scenario: saying yes is per operation, not a blanket
    Given two changes, one of which drops a column
    When a caller allows the drop by naming it
    Then both are applied
    And allowing one drop does not allow the next

  Scenario: how many rows it would lose is in the answer
    Given a column with rows in it
    When the library says the drop loses data
    Then it says how many rows carry a value in that column
