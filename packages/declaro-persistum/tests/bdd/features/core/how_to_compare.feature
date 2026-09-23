@bdd @specification
Feature: how_to_compare -- How one engine writes one comparison
  Fifteen operators and three engines. The differences live in a table, so the
  renderer reads an answer rather than knowing fifteen operators itself.

  Scenario: an ordinary comparison is a symbol and one value
    Given the equals operator on any engine
    Then the answer gives a symbol and says one value goes beside it

  Scenario: matching part of the text wraps the value
    Given the contains operator on any engine
    Then the answer says the value is wrapped in a pattern

  Scenario: asking whether something is absent takes no value at all
    Given the is_null operator on any engine
    Then the answer says no value goes beside the symbol

  Scenario: a range takes two
    Given the between operator on any engine
    Then the answer says two values go beside the symbol, a floor and a ceiling

  Scenario: ignoring case is the engine's own word for it
    Given any text operator on every engine
    Then each engine says what it writes instead when case does not matter

  Scenario: every operator answers on every engine
    When all fifteen operators are asked of all three engines
    Then all forty-five pairs answer

  Scenario: an operator nobody wrote down is a fault, not a guess
    Given an operator the table does not cover
    Then a fault comes back naming it and the engine
