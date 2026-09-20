@bdd @specification
Feature: room_post_receipt -- Leaving the outcome where its caller will find it
  The drainer never speaks to the caller. It puts what happened in the room, and
  the caller collecting takes it from there.

  Scenario: a receipt reaches the caller waiting for it
    When a drainer posts an outcome for a ticket
    Then the caller waiting on that ticket is given it

  Scenario: an outcome nobody is waiting for is logged
    When a drainer posts an outcome for a ticket whose caller has gone
    Then it is written to the log
    And nothing else is kept
