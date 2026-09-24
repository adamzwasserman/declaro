@bdd @specification
Feature: session_close_connection -- What was opened is closed
  `docs/architecture.md:314` ends shutdown with the close, after the push. A crew
  holding one connection per drainer for life means a connection left open is an
  OS thread left running.

  # THREE SCENARIOS MOVED OUT ON 2026-09-24, TO THE CARD THAT CAN ANSWER THEM.
  # This one takes a connection and answers NOTHING or a `Fault`, so it is handed
  # one connection and knows nothing about how many there were or what ran
  # before it. Each of the three was about the shutdown around this call:
  #
  # "every connection opened at open is closed at shutdown" and "closing happens
  # after the push, never before" -> `lifecycle_shut_down.feature`. Which
  # connections there are and what order the steps run in is that orchestrator's
  # whole job, and stating it here would say this door counts something.
  #
  # "a connection is closed even when the work fails" -> the caller's, wherever
  # the work is. This door is not reached at all if nobody calls it, which is a
  # fact about the caller rather than about closing.

  @precommit
  Scenario: the connection handed over comes back shut
    Given a connection that is open
    When it is closed
    Then the connection is shut
    And nothing came back with it

  @precommit
  Scenario: a close that fails is returned, not swallowed
    Given a connection the driver refuses to close
    When it is closed
    Then a fault comes back naming what the driver said

  # CLOSING A CONNECTION THAT IS ALREADY SHUT IS NOT A FAILURE. Shutdown may be
  # reached twice -- a signal and then an ordinary exit -- and the second pass
  # finds connections the first one closed. A refusal there would make an
  # ordinary shutdown read as a broken one, every time.
  @precommit
  Scenario: closing one that is already shut is quiet
    Given a connection that has already been closed
    When it is closed
    Then nothing came back with it
