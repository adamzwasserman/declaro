@bdd @specification
Feature: log_write -- What an operator reads afterwards
  `docs/architecture.md:114`: a failure comes back as a value, never as a log
  line. A function that logs an error and hands you something you cannot tell
  from success has two outputs and only one of them is in the signature.

  Scenario: a failure is returned, not logged
    When a function cannot do its job
    Then the fault came back to its caller

  Scenario: what is logged is what nobody is waiting on
    Given a write whose caller abandoned its ticket
    When it fails
    Then the failure is logged, because there is nobody left to return it to

  Scenario: a pure function never reaches this
    When the pure core runs
    Then nothing in it wrote a line
