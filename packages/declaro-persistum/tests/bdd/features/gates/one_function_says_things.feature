@bdd @specification
Feature: One function says things out loud
  Information goes through one declared function of ours. Everything else calls that
  one. `logging.getLogger(__name__)` reaches a global nobody declared and cannot be
  substituted, so twenty-four call sites become twenty-four independent edges. One
  declared function is one edge, and it decides format, level, destination, and
  whether to write at all.

  THIRTEEN MODULES EACH CALLED `logging.getLogger` BEFORE, which is why the rule
  exists. Nothing stops the fourteenth.

  A FAILURE IS RETURNED, NEVER WRITTEN, which is the other half. A function that logs
  an error and hands back something the caller cannot tell from success has two
  outputs and only one is in its signature.

  Scenario: a module importing logging directly is refused
    Given a module that imports `logging` and calls `getLogger`
    When the gate runs
    Then it fails and names that module

  Scenario: the one declared function may import logging
    Given the module that declares the saying function
    When the gate runs
    Then it passes, and it is the only module allowed to

  Scenario: the allowance is a list with reasons, not a pattern
    When the gate's allowance is read
    Then every name on it carries why it ships unrouted
    And the list is a ratchet that only shrinks

  Scenario: a failure written instead of returned is refused
    Given a function that catches an error, logs it, and returns a value
    When the gate runs
    Then it fails, because the caller cannot see what happened

  Scenario: a failure returned as a value passes
    Given a function that catches an error and returns it
    When the gate runs
    Then it passes

  Scenario: the saying function refuses a keyword the standard library owns
    When a caller passes `exc_info` as a fact
    Then it is refused and told to pass the cause instead

  Scenario: the gate counts what it read
    When the gate runs over the package
    Then it reports how many modules it read
    And a run that read nothing fails rather than passing
