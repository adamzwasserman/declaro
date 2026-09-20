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
