@bdd @specification
Feature: channel_push -- Sending local commits up the channel
  One direction of the one channel to the primary. `docs/architecture.md:23`
  insists this is not a write to the journal: it carries a log, and it bounds
  nothing about how many writes can be in flight.

  # RENAMED FROM `turso_push.feature` ON 2026-09-24, with the scenarios below
  # rewritten to name no engine. The card is `channel_push` and takes a
  # `Channel`, which is a handle this door never opens; Turso is the only engine
  # that has one today and the door does not know that. A feature named after a
  # database type is a feature that has to be rewritten when a second engine
  # grows a channel.

  @network
  Scenario: local commits reach the primary
    Given local commits the primary has not seen
    When a push runs
    Then the primary has them

  @network
  Scenario: a push with nothing to send is quiet
    Given a channel with nothing waiting on it
    When a push runs
    Then nothing comes back, and it is not a fault

  # THIS SCENARIO IS ALSO HOW A DEAD PRIMARY IS CAUGHT AT OPEN, since 2026-10-10.
  # `boundary_out fn session_open_channel` used to pull to prove the channel and
  # does not any more: the driver builds its sync half on the first push or pull,
  # so before that call a channel to a primary that does not exist is the same
  # object as a live one. The open's own push is that first call, and this is what
  # it answers with.
  Scenario: a push the primary refuses is a fault, not a silence
    Given a channel the primary will not take
    When a push runs
    Then a fault comes back saying what to do about it

  # MOVED HERE FROM `session/session_open_channel.feature` ON 2026-10-10. Making a
  # copy where none exists is not something the open does: it makes the folder and
  # the main file and sends nothing. The first push is what brings the primary
  # down, measured that day at twenty-three tables with the pull after it bringing
  # nothing. So a cold start waits on the primary once, for the push it has to
  # make anyway.
  @network
  Scenario: a push fills a copy that has nothing on it yet
    Given a primary holding a table this copy has never seen
    When a push runs
    Then the table is readable on this copy without asking for anything else

  # THE LOOP IS WHAT A CALLER IS ON, AND A BLOCKING PUSH STOPS ALL OF IT. The
  # driver's push blocks until the primary answers, which is a round trip over
  # a network. Run on the loop it holds every other request in the process for
  # that whole time, which is the one thing `persistum-nonblocking-cloud-open`
  # says must never happen.
  @network
  Scenario: a push does not stop everything else in the process
    Given a channel and other work waiting to run
    When a push runs
    Then the other work ran while the push was in flight
