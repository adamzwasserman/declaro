@bdd
Feature: When a replica decides a replication pass is worth taking
  As a service holding a local copy of a remote database
  I want a pass only when there is something to send and nobody waiting to write
  So that replication never takes the connection a request is queued for

  # ONE SCENARIO PER FUNCTION on the path from "a commit happened" to "a pass
  # ran". Every one is pure: a database record in, an answer out.
  #
  # WHY THIS FILE EXISTS. Three tests replaced `replicate` on the module and
  # counted the calls. The dispatch table binds that function when the module
  # loads, so the replacement never reached the code under test: all three
  # patches were dead lines that read as the test's mechanism. Measured
  # 2026-08-27 by deleting them, after which every test in that file passed.

  @precommit
  Scenario: is_replicated says a database with a primary is a replica
    Given a database with a primary
    Then is_replicated answers yes

  @precommit
  Scenario: is_replicated says a database with no primary is not
    Given a database with no primary
    Then is_replicated answers no

  @precommit
  Scenario: writers_waiting counts nobody when no lock is held
    Given a database with no primary
    Then writers_waiting counts 0

  @precommit
  Scenario: nothing_happened answers no rather than failing
    Given a database with no primary
    When nothing_happened is asked to run a pass
    Then it answers that no pass ran

  @precommit
  Scenario: replicate_if_idle drops the pass on a database with no primary
    Given a database with no primary
    When replicate_if_idle is asked to run a pass
    Then it answers that no pass ran

  @precommit
  Scenario: replicate_if_idle drops the pass while a writer is waiting
    Given a database with a primary and one writer waiting
    When replicate_if_idle is asked to run a pass
    Then it answers that no pass ran

  @precommit
  Scenario: replicate_if_idle drops the pass on a closed database
    Given a database with a primary that has been closed
    When replicate_if_idle is asked to run a pass
    Then it answers that no pass ran

  @precommit
  Scenario: replicate_if_idle takes the pass when nobody is waiting
    Given a database with a primary
    When replicate_if_idle is asked to run a pass
    Then it answers that a pass ran

  @precommit
  Scenario: replicate sends before it receives
    Given a database with a primary
    When replicate runs one pass
    Then it sent before it received

  @precommit
  Scenario: _replication_stands accepts a policy that has somewhere to send
    When _replication_stands is asked about "replicate" from "open_turso"
    Then it accepts

  @precommit
  Scenario: _refuse_replicating_nowhere refuses at open rather than at SIGTERM
    When _refuse_replicating_nowhere is asked about "replicate" from "open_sqlite"
    Then it refuses and names "open_sqlite"

  @precommit
  Scenario: _nothing_to_deliver answers zero passes for a local-only database
    Given a database with no primary
    When _nothing_to_deliver is asked to deliver before exit
    Then it answers 0 passes

  # NOTHING IS ON A CLOCK. The loop waits on an event a commit sets, so a
  # database with nothing pending takes no connection and burns no round trip.
  #
  # These two used to count calls to a `replicate` patched onto the module.
  # `test_a_commit_wakes_the_loop` had already been rewritten on 2026-08-22
  # after that patch was found to have stopped working silently; its three
  # siblings kept the dead patch for five more days.

  @precommit
  Scenario: replication_loop takes no pass while nothing is pending
    Given a database with a primary
    When the loop runs with nothing pending
    Then no pass ran

  @precommit
  Scenario: replication_loop takes a pass once a commit says so
    Given a database with a primary
    When the loop runs and a commit says there is work
    Then a pass ran

  # THE TWO DIRECTIONS A CALLER CAN ASK FOR BY NAME. `flush` blocks until local
  # writes have reached the primary; `refresh` brings the primary's changes
  # down. Both are one line: look up whether this database has a primary, and
  # call. The table and the words were the only things that differed.
  #
  # ON A DATABASE WITH NO PRIMARY BOTH RETURN AT ONCE, and that is an answer
  # rather than an omission. A local file has nowhere to send and nothing to
  # fetch, and a caller that awaited either has been told so.

  @precommit
  Scenario: flush sends local writes and waits for them to land
    Given a database with a primary
    When flush is awaited
    Then it sent before it received

  @precommit
  Scenario: flush on a database with no primary returns at once
    Given a database with no primary
    When flush is awaited
    Then nothing was sent or received

  @precommit
  Scenario: refresh brings the primary's changes down
    Given a database with a primary
    When refresh is awaited
    Then it received

  @precommit
  Scenario: refresh on a database with no primary returns at once
    Given a database with no primary
    When refresh is awaited
    Then nothing was sent or received

  # THE THREE ANSWERS AN ENGINE WITH NOTHING TO DO STILL HAS TO GIVE. Every
  # Database carries these as callables, so a local file cannot decline the
  # question; it answers, and the answer is the point.
  #
  # THEY ARE THREE ANSWERS, NOT ONE. Zero passes ran, everything is delivered,
  # and no pass happened are different facts of different types, and each is
  # the do-nothing half of a different pair.

  @precommit
  Scenario: no_passes says zero passes ran
    Given a database with no primary
    When no_passes is asked how many passes ran
    Then it answers 0 passes

  @precommit
  Scenario: already_delivered says everything has arrived
    Given a database with no primary
    When already_delivered is asked whether the primary has it all
    Then it answers yes

  @precommit
  Scenario: nothing_happened says no pass ran
    Given a database with no primary
    When nothing_happened is asked to run a pass
    Then it answers that no pass ran

  # A HANDLER THAT WILL NOT COME OFF. Putting a signal handler back can be
  # refused by the platform: off the main thread it raises, and that is the
  # real case rather than an invented one.
  #
  # THE COUNT IS HOW ANYONE FINDS OUT. `restore` says how many it put back, and
  # a caller comparing that against how many went on is the only way to learn
  # that one is still installed. A handler left on after close is what drives a
  # released database when a SIGTERM arrives later.

  @precommit
  Scenario: putting a handler back reports that it went back
    Given a signal handler this module installed
    When it is put back
    Then it reports one handler restored

  @precommit
  Scenario: a platform that refuses says so rather than reporting success
    Given a signal handler this module installed
    When it is put back somewhere the platform refuses
    Then it reports no handler restored

  @precommit
  Scenario: a handler that will not come off is not reported as closed cleanly
    Given a database that trapped the shutdown signal
    When the handlers are put back somewhere the platform refuses
    Then it refuses, naming the handler still installed
