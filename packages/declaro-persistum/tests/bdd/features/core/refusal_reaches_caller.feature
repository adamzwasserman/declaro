@bdd @specification
Feature: refusal_reaches_caller -- Raised, or handed back as a receipt
  A caller that cannot tell them apart will retry the one that can never succeed
  and give up on the one that would have worked.

  Scenario: a constraint violation is raised
    Given a write the engine refused outright
    Then it is raised to the caller that wrote it

  Scenario: a spent budget is a receipt
    Given a write that only ever met contention
    Then it comes back as a receipt saying so

  Scenario: the kind decides, never the message
    Then nothing read the text of the failure
