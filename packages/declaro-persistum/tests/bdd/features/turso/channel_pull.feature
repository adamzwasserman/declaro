@bdd @specification
Feature: channel_pull -- Bringing down what this copy does not have
  The other direction of the one channel. The program asks; nothing arrives on
  its own, which is what makes this a reader rather than a door.

  # RENAMED FROM `turso_pull.feature` ON 2026-09-24, and five scenarios moved
  # out with it. The card is `channel_pull` and takes a `Channel`, so what it
  # can answer is what one pull does. What went, and where:
  #
  # "a cold open copies the whole database" and "a warm open does not block" ->
  # `session_connect.feature`. Both are about when the open calls this, which is
  # that orchestrator's decision; this door pulls whenever it is asked.
  #
  # The three provisioning scenarios -> `session_open_channel.feature`. Making a
  # copy where none exists is opening the channel, not pulling down it, and the
  # card that takes a target and a primary is the one that can say so.

  @network
  Scenario: what the primary has and this copy does not comes down
    Given a primary holding rows this copy has never seen
    When a pull runs
    Then this copy has them

  @network
  Scenario: a pull with nothing to bring down is quiet
    Given a copy already level with the primary
    When a pull runs
    Then nothing comes back, and it is not a fault

  Scenario: a pull the primary refuses is a fault, not a silence
    Given a channel the primary will not take
    When a pull runs
    Then a fault comes back saying what to do about it

  # THE SAME REASON AS THE PUSH. The driver's pull waits for the primary to
  # answer, and a wait taken on the loop is every other request in the process
  # waiting with it.
  @network
  Scenario: a pull does not stop everything else in the process
    Given a channel and other work waiting to run
    When a pull runs
    Then the other work ran while the pull was in flight
