@bdd @specification
Feature: lifecycle_turn -- The runtime hands over a spare moment
  Nothing outside asked for this. It is the door a background turn arrives at,
  and the only way a deposited write or an opportunistic sync ever happens.

  Scenario: a turn drains what is waiting
    Given three writes in the room
    When a turn arrives
    Then they run in arrival order

  Scenario: a turn with nothing waiting syncs instead
    Given an empty room and a replicated database
    When a turn arrives
    Then the sync pass ran
