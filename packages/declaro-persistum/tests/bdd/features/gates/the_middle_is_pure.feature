@bdd @specification
Feature: The middle is pure
  Diffing, ordering, ambiguity detection, validation and SQL rendering are
  functions over data, and none of them touches the world. The declaration says
  which are which: a plain `fn` in `docs/design/persistum.hd` is pure, and every
  function that reaches outside the process carries a role that says so.

  # REWRITTEN 2026-09-25. Every scenario here described a gate that read
  # `@helper` and `@boundary` decorators, and no such decorator exists anywhere
  # in this package. It was written for the source that was deleted on
  # 2026-09-21. The declaration marks purity now, by role, so the gate reads the
  # declaration and the modules named in it. Nothing was checking this at all
  # until today, and the whole architecture rests on it.

  I/O IS NOT THE ONLY WAY OUT. A function that reads a clock, a random source or
  an environment variable is impure even though it opened no connection. A
  function whose answer changes between two identical calls is not pure,
  whatever it touched to make that happen.

  Scenario: every pure function in the declaration is pure in the code
    When the gate reads the declaration and every module it names
    Then no plain `fn` reads a clock, a random source or the environment
    And no plain `fn` opens a file, a connection or a socket

  Scenario: a pure function does not reach a boundary
    When the gate reads the declaration and every module it names
    Then no plain `fn` imports a function the declaration marks as a boundary

  Scenario: a boundary may do all of those
    When the gate reads the declaration and every module it names
    Then a function declared `boundary_in`, `boundary_out` or `reader` is not reported

  Scenario: the gate finds a planted impurity of every kind it claims to catch
    Given one planted pure function for each impurity the gate names
    When the gate runs over them
    Then it finds every one of them

  Scenario: the gate can see the functions it claims to check
    When the gate reads the declaration and every module it names
    Then it found a module for every plain `fn` the declaration declares
    And that count is more than forty
