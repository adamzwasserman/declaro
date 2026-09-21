@bdd @specification
Feature: condition_from_narrowing -- What a caller wrote becomes what the library holds
  A caller narrows a question in the words their grammar gave them. This library
  holds one shape: every comparison spelled out, with `any_of` and `none_of`
  present whether or not anybody used them. This is the one place the first
  becomes the second.

  WHY IT IS ONE FUNCTION AND NOT FOUR. Measured 2026-09-21, after the four
  grammar builders were written separately from their own features: all four had
  invented this same work, and two of them had already drifted from the other two
  on questions nobody had decided. A condition is data, and data composes the
  same way whatever spelled it.

  # ------------------------------------------------------ what a bare value means

  Scenario: a bare value is an equality
    When a caller narrows by name being "bob"
    Then the condition compares name with eq against "bob"

  Scenario: the two spellings of an equality are one condition
    When one caller writes the value plainly and another names eq
    Then both produce the same condition

  Scenario: two keys mean both
    When a caller narrows by name and by age at once
    Then the condition carries a comparison for each, and asks for both

  # ------------------------------------------------------------ naming an operator

  Scenario: an operator the caller named is the operator carried
    When a caller narrows by age being greater than 18
    Then the condition compares age with gt against 18

  Scenario: an operator taking two values carries both
    When a caller narrows by age being between 18 and 30
    Then the condition compares age with between against both numbers
    And neither number was pasted into anything

  Scenario: an operator nobody declared is refused
    When a caller narrows by an operator this library does not have
    Then the refusal names what they wrote and lists the operators that exist

  Scenario: a comparison naming two operators is refused
    When a caller writes one comparison naming both gt and lt
    Then the refusal says a comparison names one operator
    And it says two keys are how you ask for both

  Scenario: a comparison naming no operator at all is refused
    When a caller writes a comparison with nothing in it
    Then the refusal says a comparison names one operator

  # ------------------------------------------------------- whether case matters

  Scenario: text matching is case-sensitive unless the caller says otherwise
    When a caller narrows by name being like "bo%"
    Then the comparison says case matters

  Scenario: a caller who says case does not matter is carried
    When a caller narrows by name being like "bo%", ignoring case
    Then the comparison says case does not matter

  # THIS IS NOT A DEFAULT, AND THE DIFFERENCE IS THE WHOLE RULE. A comparison
  # arriving without the companion is declared case-sensitive here, once, rather
  # than left for three engines to decide differently further down.
  Scenario: the companion is decided here, not left to the engine
    When a caller narrows by name being like "bo%"
    Then the comparison carries an answer about case either way

  # ------------------------------------------------------------ or, and not

  Scenario: or becomes a list of alternatives
    When a caller narrows by name being "alice" or "carol"
    Then the condition holds those two as alternatives
    And the word or appears nowhere in it

  Scenario: not becomes a list of exclusions
    When a caller narrows by name not being "bob"
    Then the condition holds that one as an exclusion
    And the word not appears nowhere in it

  Scenario: alternatives and exclusions nest
    When a caller writes an alternative that itself narrows two ways
    Then each level is a condition of the same shape

  Scenario: a caller who narrowed nothing gets an empty condition
    When a caller narrows by nothing
    Then the condition compares nothing, and both its lists are empty

  # ----------------------------------------------------- what cannot be carried

  Scenario: a narrowing given as text is refused
    When a caller writes their narrowing as a sentence
    Then the refusal says a condition is data
    And nothing was pasted into a statement

  Scenario: a shape this library has no rule for is refused by its own shape
    When a caller narrows by something that is neither a value nor a comparison
    Then the refusal names the shape they wrote

  # --------------------------------------------------------------- what it is not

  Scenario: it knows no grammar and no engine
    When four callers ask one question in four grammars
    Then the four narrowings produce one condition
