@bdd @specification
Feature: lifecycle_catch_up -- What a free turn is spent on
  Two uses of one spare moment, and they are not related to each other: empty the
  room, and if nothing else wants the connection, push and pull.

  Scenario: a writer waiting drops the sync pass
    Given a writer waiting for the connection
    When a turn arrives
    Then no sync pass ran

  Scenario: a dropped pass is dropped, not queued
    Given ten passes dropped under load
    When the load clears
    Then one pass runs, not ten

  Scenario: the room drains one write at a time, in arrival order
    Given three writes deposited in a known order
    When a turn arrives
    Then they ran in that order

  Scenario: a conflict is retried and a refusal is not
    Given one write that conflicts once and one the engine refuses
    When a turn arrives
    Then the first ran on a second attempt and the second was recorded against its caller
