@bdd @specification
Feature: session_open_channel -- The one line to the primary
  There is exactly one channel per copy, measured 2026-08-12 and recorded in
  `docs/design/mvcc-connection-rule.md`. That bounds the channel and nothing
  else: a write goes to the local journal and the channel ships it later.

  # THREE SCENARIOS ARRIVED FROM `channel_pull.feature` ON 2026-09-24. Making a
  # copy where none exists is opening the channel, not pulling down one, and
  # this is the card that takes a target and a primary and can say so.

  # THE `@network` TAGS CAME OFF ON 2026-10-10, BECAUSE THIS OPEN REACHES NO
  # PRIMARY. Measured that day: the driver builds its sync half on the first push
  # or pull, so the open itself makes files and sends nothing. The scenarios below
  # look at the disk and at this door's own source.
  #
  # THEY STILL NAME THE REAL PRIMARY, AND THAT IS WHAT MAKES THEM A CHECK. Put a
  # pull back in this door and it would land, bring the primary down, and make the
  # driver's three sidecars, which the third scenario says are not there. Against
  # a primary that does not exist the restored pull would fail instead and the
  # files would look the same, so nothing would notice.
  Scenario: a copy is made where the folder already exists
    Given a path in a folder that exists, with no copy at it yet
    When a channel is opened at that path
    Then a copy is there

  Scenario: a copy is made where the folder does not exist yet
    Given a path whose folder does not exist
    When a channel is opened at that path
    Then the folder was made
    And a copy is there

  # THE COPY IS A MAIN FILE AND THREE SIDECARS, AND THE OPEN MAKES ONLY THE MAIN
  # FILE. The changes, info and write-ahead files arrive with the first push or
  # pull, measured 2026-10-10. So a path holding a main file alone is a copy
  # nothing can read yet, and whoever asks whether a copy is here has to ask
  # about all four.
  Scenario: the open makes the file and the first push makes the rest
    Given a path in a folder that exists, with no copy at it yet
    When a channel is opened at that path
    Then the main file is there and the three the driver needs are not
    And the open made no round trip

  # TWO SCENARIOS MOVED TO `turso/channel_push.feature` ON 2026-10-10, because
  # this door stopped proving the channel with a pull. They were "a cold open
  # brings the primary's schema down with it" and "a primary that cannot be
  # reached is a fault, not a handle". Both are claims about a round trip, and the
  # only round trip left at open is the push: it raises the same NoneType on a
  # dead primary, and on a copy with nothing on it yet it brings the primary's
  # whole schema down. A pull here spent a trip the open then spent again.
  #
  # WHAT STOPS HERE IS MAKING THE FOLDER, AND ONLY THAT. Measured 2026-10-10: the
  # driver's open does so little that a path this process cannot write hands back
  # a channel anyway, because nothing is written until the first push. The one
  # call that happens for real is the folder, so that is the one failure this door
  # can report.
  Scenario: a folder this process cannot make is a fault, not a handle
    Given a path needing a folder inside one this process may not write
    When a channel is opened at that path
    Then a fault comes back naming the path
    And no channel is handed back
