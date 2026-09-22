@bdd @specification
Feature: room_release_ticket -- Waiting for one ticket and taking it out
  The caller is parked here until the drainer posts what happened, which is why a
  deposit could return at once.

  Scenario: it answers when the outcome is posted
    When a caller waits on a ticket and the drainer posts its outcome
    Then the outcome comes back to that caller

  Scenario: it takes the ticket out of the room
    When an outcome has been handed back
    Then the room holds nothing for that ticket

  Scenario: one ticket, one caller
    When two callers wait on two tickets and one write fails
    Then only that ticket's caller is told

  # ADDED 2026-09-22, WITH THE FUNCTION. A collected ticket leaves the room, so
  # collecting the same one twice asks about a ticket the room has never heard
  # of, and so does a ticket nobody deposited under. Without an answer for that,
  # the second call waits for an outcome nobody will ever post and the caller
  # never comes back.
  Scenario: a ticket the room does not know
    When a caller collects on a ticket that was never deposited
    Then it is refused at once rather than waiting
