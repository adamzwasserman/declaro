@bdd @specification
Feature: worth_retrying -- Whether a failure is worth another attempt
  A conflict at commit is retried. A refusal is handed back at once. The
  difference is asked of the failure's kind, never of its message.

  Scenario: a conflict is retried
    Given a failure of the conflict kind
    Then it is worth trying again

  Scenario: a refusal is not
    Given a failure the engine refused outright
    Then it is handed back at once

  Scenario: the message is never read
    Given a conflict whose message says nothing about conflicts
    Then it is still retried

  # ------------------------------------------------------------------
  # FOLDED IN FROM `running/retrying_is_the_librarys_job.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # Under concurrent writes a conflict fires at commit, on a row two writers both wrote.
  # Retries do not disappear, they move. Backoff with jitter is the library's work by
  # the ownership test: it could go differently on a different engine, so it is not
  # yours.

  Scenario: a conflict is retried, and a refusal is not
    Given a write that conflicts at commit
    And a write that violates a constraint
    Then the first is retried and the second is handed back at once
