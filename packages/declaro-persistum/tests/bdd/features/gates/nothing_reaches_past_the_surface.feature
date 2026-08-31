@bdd @specification
Feature: Nothing reaches past the public surface
  The rules cover what the package exports. A caller can import
  `declaro_persistum.query.executor` and use what is inside, and every rule about
  strings, connections and engines stops applying the moment they do.

  A closed vocabulary that anyone can step around is not closed.

  Scenario: importing a submodule directly is refused
    When a caller imports a module that is not the package itself
    Then it is refused, and the refusal names what to import instead

  Scenario: the package's own modules import each other freely
    When the package imports its own modules
    Then nothing is refused, because the rule is about callers

  Scenario: a name not exported is not reachable through the package
    When a caller reaches for a name the package does not export
    Then it is not there

  Scenario: the exported names are one list
    When the public surface is read
    Then it is one declared list
    And a name absent from it is absent from the package

  Scenario: the gate counts what it read
    When the gate runs
    Then it reports how many modules it checked
    And a run that checked nothing fails rather than passing

  Scenario: a test may reach inside, and says so
    Given a test importing an internal module
    Then it is allowed, and the allowance is a declared list with reasons
