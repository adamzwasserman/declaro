@bdd @specification
Feature: Nothing takes a default
  A default absorbs the caller's omission. Afterwards nothing can tell a caller who
  chose from a caller who forgot, and the path nobody chose is the path nobody tested.
  It is catch-and-swallow pointed at the inputs, and it manufactures an unexercised
  region of the function by construction.

  THE HONEST CODE PRINCIPLES CALL THIS THE MOST-CITED AND LEAST-DEFENDED RULE IN THE
  DOCUMENT: "a defaulted argument will pass every gate we run." This is the gate.

  Absence is declared instead, as `Nothing`, and resolved once in a visible step with
  `resolved(given, otherwise)`.

  Scenario: a defaulted parameter is refused
    Given a function declaring `timeout: int = 30`
    When the gate runs
    Then it fails and names that function and that parameter

  Scenario: a mutable default is refused the same way
    Given a function declaring `items: list = []`
    When the gate runs
    Then it fails and names that function and that parameter

  Scenario: a None default is still a default
    Given a function declaring `token: str | None = None`
    When the gate runs
    Then it fails, because None cannot be told from a caller who forgot

  Scenario: declared absence is accepted
    Given a function declaring `token: str | Nothing` with no default
    When the gate runs
    Then it passes

  Scenario: the gate counts, so zero offenders is not zero found
    When the gate runs over the package
    Then it reports how many functions it read
    And a run that read nothing fails rather than passing

  Scenario: a closure over an enclosing name is not a default
    Given a nested function whose parameter defaults to a name from its enclosing scope
    When the gate runs
    Then it is allowed, and the allowance is recorded with its reason
