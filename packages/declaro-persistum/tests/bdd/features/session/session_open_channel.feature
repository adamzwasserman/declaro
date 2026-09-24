@bdd @specification
Feature: session_open_channel -- The one line to the primary
  There is exactly one channel per copy, measured 2026-08-12 and recorded in
  `docs/design/mvcc-connection-rule.md`. That bounds the channel and nothing
  else: a write goes to the local journal and the channel ships it later.

  # THREE SCENARIOS ARRIVED FROM `channel_pull.feature` ON 2026-09-24. Making a
  # copy where none exists is opening the channel, not pulling down one, and
  # this is the card that takes a target and a primary and can say so.

  @network
  Scenario: a copy is made where the folder already exists
    Given a path in a folder that exists, with no copy at it yet
    When a channel is opened at that path
    Then a copy is there

  @network
  Scenario: a copy is made where the folder does not exist yet
    Given a path whose folder does not exist
    When a channel is opened at that path
    Then the folder was made
    And a copy is there

  @network
  Scenario: a cold open brings the primary's schema down with it
    Given a primary holding a table this path has never seen
    When a channel is opened at that path
    Then the table is readable without asking for anything else

  # A PRIMARY THAT IS NOT THERE OPENS ANYWAY, AND THAT IS THE DRIVER'S DOING.
  # Measured 2026-09-24: pyturso hands back a connection for a primary that does
  # not exist, with nothing different about the object, and every push and pull
  # on it then raises about a NoneType. A door that handed that back would be a
  # database that looks replicated, writes happily for a day, and loses all of it
  # at the first shutdown.
  Scenario: a primary that cannot be reached is a fault, not a handle
    Given a primary that does not exist
    When a channel is opened to it
    Then a fault comes back naming the primary
    And no channel is handed back
