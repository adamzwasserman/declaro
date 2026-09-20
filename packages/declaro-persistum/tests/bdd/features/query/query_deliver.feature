@bdd @specification
Feature: query_deliver -- Waiting for a drainer and handing back the receipt
  One orchestrator, reached by one door. It parks the caller until the drainer
  posts what happened, hands back the receipt and drops the ticket.

  Scenario: it waits for the drainer
    When a caller collects a ticket the crew has not reached yet
    Then it waits, and answers once the write has run

  Scenario: a failure whose caller has gone is logged instead
    When a write fails and its caller abandoned the ticket
    Then the drainer logs it
    And that is the only place it can still be seen

  Scenario: the room is empty again afterwards
    When a receipt has been handed back
    Then nothing for that ticket is left in the room
