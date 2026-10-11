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

  # IT ANSWERS YES OR NO AND NEVER A FAULT. A path that cannot be read holds no
  # copy this library can use, which is the same answer the caller acts on, so
  # there is nothing a refusal would add.

  # MEASURED 2026-10-10 against the staging primary, and three facts decide where
  # this reader is asked and what its answer is worth.
  #
  # `turso.aio.connect(path)` MAKES THE FILE. A path with nothing at it holds an
  # empty database a moment later. So the disk has to be asked before any
  # connection opens, and `orchestrator fn session_connect` opens its reader and
  # its writers well before the channel. Asked where the channel work happens,
  # this reader answers yes on a machine that started with nothing.
  #
  # A PUSH AS THE FIRST CALL ON A COLD REPLICA FILLS IT. The cold copy held all
  # twenty-three of the primary's tables after the push alone, and the pull after
  # that push brought down nothing. The driver builds its sync half on the first
  # call, whichever call that is, and `bootstrap_if_empty` does the filling.
  #
  # A PUSH PROVES THE CHANNEL EXACTLY AS A PULL DOES. On a primary that does not
  # exist it raises the same `AttributeError: 'NoneType' object has no attribute
  # 'push'`, because the driver never built the sync half and never said so.
  # Before either call, a dead channel and a live one are the same object with
  # `_conn` set to None, so nothing can be told apart without a round trip.
  #
  # WHAT THAT LEAVES OPEN, AND IT IS THE AUTHOR'S CALL. The two scenarios below
  # say a warm copy is not filled again, on the reading that a warm copy is
  # current. It is not: several services share one primary here, so a copy that
  # was already on this machine is the one that may be behind, and a cold copy is
  # the one the push has already filled. The saving and the staleness are the
  # other way round from what these two scenarios say.

  Scenario: a file that is there answers yes
    Given a database file already on this machine
    When copy_on_disk is asked about its path
    Then it answers yes

  # IT IS ASKED ABOUT THE TARGET A CALLER WROTE, AND THE SCHEME IS PART OF THAT.
  # `orchestrator fn session_connect` holds the target and nothing else when it
  # asks, because it asks before it opens anything.
  Scenario Outline: a file written as a "<scheme>" target answers yes
    Given a database file already on this machine, written as a "<scheme>" target
    When copy_on_disk is asked about it
    Then it answers yes

    Examples:
      | scheme |
      | sqlite |
      | libsql |

  # A TARGET NAMING A SERVER HAS NO COPY ON ANY DISK, and no answer here is the
  # one a caller acts on either: only the two engines that live on a disk ask.
  Scenario: a target naming a server answers no
    Given a target naming a postgresql server
    When copy_on_disk is asked about it
    Then it answers no

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

  # TWO SCENARIOS WENT TO `session/session_connect.feature` ON 2026-10-10, where
  # the order they describe lives. They were "a warm copy is not filled again" and
  # "a cold copy is filled", and they said the saving backwards: a copy that was
  # already here is the one that can be behind the primary, because several
  # services share it, and a copy that was not here is filled by the push the open
  # makes anyway. So the pull runs for the warm start and not the cold one, which
  # is the opposite of what those two asked for.
  #
  # WHAT THIS CARD ANSWERS IS YES OR NO. What an open does with the answer is the
  # open's business, and a scenario here that opened a database would be this
  # feature describing another card's order.
