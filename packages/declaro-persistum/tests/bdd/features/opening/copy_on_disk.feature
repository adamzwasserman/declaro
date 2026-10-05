@bdd @specification
Feature: copy_on_disk -- Is there already a copy here
  The calling program cannot know whether a file is on this machine, so the library
  looks. It derives the path from the target and asks the disk.

  # WRITTEN 2026-10-05. Until today nothing asked. `boundary_in fn
  # session_open_channel` opened the channel and then pulled, every single time,
  # and `orchestrator fn session_connect` pulled again after it. On a cold open the
  # first pull is the filling the deployment needs anyway. On a warm one it costs a
  # round trip to the primary for nothing, measured at about two seconds against
  # the staging primary. Nothing in the declaration asked whether a copy was there,
  # so filling one was assumed rather than chosen.

  IT ANSWERS YES OR NO AND NEVER A FAULT. A path that cannot be read holds no copy
  this library can use, which is the same answer the caller acts on, so there is
  nothing a refusal would add.

  Scenario: a file that is there answers yes
    Given a database file already on this machine
    When copy_on_disk is asked about its path
    Then it answers yes

  Scenario: a path with nothing at it answers no
    Given a path where no file has been made
    When copy_on_disk is asked about it
    Then it answers no

  Scenario: a folder that does not exist answers no rather than refusing
    Given a path inside a folder nobody has made
    When copy_on_disk is asked about it
    Then it answers no
    And nothing was refused

  Scenario: a path it may not read answers no
    Given a path the process has no permission to read
    When copy_on_disk is asked about it
    Then it answers no
    And a copy it cannot read is a copy it cannot use

  Scenario: it makes nothing and changes nothing
    Given a path where no file has been made
    When copy_on_disk is asked about it
    Then no file and no folder were made
    And asking is not the same as opening

  Scenario: it asks the disk rather than remembering
    Given a database file already on this machine
    When copy_on_disk is asked twice and the file is deleted between the asks
    Then it answers yes and then no

  Scenario: a warm copy is not filled again
    Given a replicated database whose copy is already on this machine
    When a caller opens it
    Then the copy was not pulled at open
    And the open did not wait on the primary

  Scenario: a cold copy is filled
    Given a replicated database with no copy on this machine
    When a caller opens it
    Then the copy was pulled once
    And the live schema was read after that pull, not before
