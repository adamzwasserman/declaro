@bdd @specification
Feature: Retrying is the library's job
  Under concurrent writes a conflict fires at commit, on a row two writers both wrote.
  Retries do not disappear, they move. Backoff with jitter is the library's work by
  the ownership test: it could go differently on a different engine, so it is not
  yours.

  Scenario: a caller never writes a retry loop
    When a caller declares a write that conflicts once and then succeeds
    Then the caller wrote no loop and saw no conflict

  Scenario: a conflict is retried, and a refusal is not
    Given a write that conflicts at commit
    And a write that violates a constraint
    Then the first is retried and the second is handed back at once

  Scenario: the difference is asked of the exception's type
    When the library decides whether to retry
    Then it asks what kind of failure it is
    And it does not read the message

  Scenario: retries back off, with jitter
    When a write is retried three times
    Then each wait is longer than the last
    And two callers retrying together do not wait in step

  Scenario: the budget is bounded and the exhaustion is an answer
    Given a write that conflicts more often than the budget allows
    Then the caller is told the budget was exhausted
    And that is distinguishable from the write being refused

  Scenario: how many times it tried is in the answer
    Given a write that succeeded on the third attempt
    Then the answer says it took three

  Scenario: the budget has no default
    When a database is opened
    Then the retry budget is part of that declaration
    And omitting it is refused rather than filled in
