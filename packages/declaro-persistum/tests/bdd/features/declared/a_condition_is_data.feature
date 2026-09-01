@bdd @specification
Feature: A condition is data
  A condition is data in every engine, so it composes the same way everywhere. It is
  never a Python expression the interpreter evaluates, and never a string.

  Section "The data is declared too" of HOW-IT-SHOULD-WORK.md.

  Background:
    Given a model Person declaring the table "people" with columns id, name and age
    And a database holding alice aged 30, bob aged 25 and carol aged 17

  Scenario: a plain equality names a column and a value
    When a caller narrows by {"name": "bob"}
    Then it is given bob alone

  Scenario: an operator is a named key, not a symbol
    When a caller narrows by {"age": {"gt": 18}}
    Then it is given alice and bob

  Scenario: two keys mean both
    When a caller narrows by {"age": {"gt": 18}, "name": "bob"}
    Then it is given bob alone

  Scenario: or is a named key carrying a list
    When a caller narrows by {"or": [{"name": "alice"}, {"name": "carol"}]}
    Then it is given alice and carol

  Scenario: the same condition means the same thing in every engine
    When the condition {"age": {"gt": 18}} is used in all four grammars
    Then all four answers are equal

  Scenario: an operator nobody declared is refused
    When a caller narrows by {"age": {"roughly": 18}}
    Then the library refuses and names the operator
    And it lists the operators it does declare

  Scenario: a condition is never a Python expression
    When a caller compares a column to a value with a Python operator
    Then nothing in the library builds a condition from it
