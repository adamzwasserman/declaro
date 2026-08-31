@bdd @specification
Feature: The middle is pure
  Diffing, ordering, ambiguity detection, validation and SQL rendering are functions
  over data, and none of them touches the world. I/O happens at named edges, each
  carrying `@boundary`, so the edges can be counted rather than argued about.

  I/O IS NOT THE ONLY WAY OUT. A function that reads a clock, a random source, an
  environment variable or the filesystem is impure and today passes, because the
  existing check reads a list of calls that reach the network or the disk. A function
  whose answer changes between two identical calls is not pure, whatever it touched
  to make that happen.

  Scenario: a helper that opens a connection is refused
    Given a function marked `@helper` that executes on a connection
    When the gate runs
    Then it fails and names that function

  Scenario: a helper that reads the clock is refused
    Given a function marked `@helper` that calls `time.time`
    When the gate runs
    Then it fails, because two identical calls can answer differently

  Scenario: a helper that reads a random source is refused
    Given a function marked `@helper` that calls `random` or `uuid4`
    When the gate runs
    Then it fails

  Scenario: a helper that reads the environment is refused
    Given a function marked `@helper` that reads `os.environ`
    When the gate runs
    Then it fails

  Scenario: a boundary may do all of those
    Given a function marked `@boundary` that reads the clock and opens a connection
    When the gate runs
    Then it passes, because a boundary is where that belongs

  Scenario: a boundary that touches nothing is refused
    Given a function marked `@boundary` that reaches outside for nothing
    When the gate runs
    Then it fails, because the marker declares an edge that is not there

  Scenario: the gate finds a planted impurity of every kind it claims to catch
    Given one planted helper for each impurity the gate names
    When the gate runs
    Then it finds every one of them
