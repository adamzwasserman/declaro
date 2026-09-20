@bdd @specification
Feature: name_what_destroys -- Saying which operations lose data
  Dropping a column loses what was in it, dropping a table loses all of it, and
  narrowing a type loses what no longer fits. The library derives these from your
  models without being asked, so it has to say which of them destroy something.

  Scenario Outline: a destructive operation is named as destructive
    When the library works out that it would <operation>
    Then the answer says that operation loses data

    Examples:
      | operation        |
      | drop a column    |
      | drop a table     |
      | narrow a type    |

  Scenario: adding is not destructive
    When the library works out that it would add a column
    Then nothing is named as losing data
