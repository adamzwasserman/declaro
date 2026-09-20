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
