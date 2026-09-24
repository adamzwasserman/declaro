@bdd @specification
Feature: lifecycle_turn -- The runtime hands over a spare moment
  Nothing outside asked for this. It is the door a drainer's turn arrives at,
  and the only way a deposited write or an opportunistic sync ever happens.

  # REWRITTEN 2026-09-24. Both scenarios said "room", and there are no waiting
  # rooms here: there is a crew, and a caller hands it a write and holds a
  # ticket. The card takes a `Draining`, which is what one drainer holds for its
  # whole life, so that is what arrives.

  Scenario: a turn runs what the crew is holding
    Given three writes deposited in a known order
    When a turn arrives
    Then they ran in that order

  Scenario: a turn with nothing waiting syncs instead
    Given a crew holding nothing and a database that replicates
    When a turn arrives
    Then the sync pass ran

  # IT IS A DOOR AND A DOOR NEVER RAISES. A drainer runs this forever, and an
  # exception reaching the runtime ends that drainer: the crew goes on holding
  # its connection, every write deposited afterwards waits for a worker that is
  # gone, and shutdown waits for a task that already died.
  Scenario: a turn that goes wrong does not end the drainer
    Given a database whose writer has been closed under it
    When a turn arrives
    Then a fault comes back and nothing was raised
