@bdd @specification
Feature: answer_from_ticket -- What a write comes back as
  A read is shaped from rows and a write from its ticket, and both end as the one
  thing a caller reads, which is what lets one door serve both halves of the
  vocabulary.

  Scenario: a deposited write answers with its ticket
    When a caller declares one new person
    Then the answer carries the ticket the room handed back

  Scenario: the caller has not waited
    Then nothing about the answer required the write to have run
