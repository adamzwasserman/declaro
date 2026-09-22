@bdd @specification
Feature: lifecycle_shut_down -- Getting every local commit to Turso before the process dies
  The one place blocking is correct. `docs/architecture.md:314`: shutdown blocks
  until Turso has everything, retrying instead of giving up, because on ephemeral
  disk anything unreplicated at exit is gone.

  Scenario: it ignores load and takes the connection
    Given a shutdown in progress with writers still working
    When shutdown replication runs
    Then it takes the connection anyway
    And the reason is that politeness during service is data loss at exit

  Scenario: it keeps trying rather than giving up
    Given Turso refusing the first two attempts
    When shutdown replication runs
    Then it tried again and finished

  Scenario: it says what it is doing while it does it
    Given a shutdown that is taking a long time
    Then every line names the database and the time elapsed

  Scenario: a shutdown cut short says so
    Given a platform grace period shorter than the replication needs
    When the process is killed first
    Then the last line says the data has not reached Turso

  Scenario: a local database has nothing to push
    Given a database with no primary
    When the process is shutting down
    Then nothing was pushed, and the connections were closed

  # ------------------------------------------------------------------
  # FOLDED IN FROM `replication.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # A local copy and its cloud primary are brought into conformity. That is replication.
  # "Sync" in this package means synchronous and nothing else.
  # THE WORD WAS ATTACHED TO HALF THE JOB, ON BOTH BRANCHES.
  # replicate(db)   sent local commits UP and never brought anything down
  # refresh(db)     brought changes DOWN and never sent anything up
  # refresh_once    called pull() ONCE, though pyturso's pull fetches one batch
  # The module comment above them said these functions "bring the two copies into
  # conformity". Neither did. A caller that only ever called `replicate` had a local copy
  # that learned nothing about its primary for as long as the process lived — and nothing
  # said so, because a function that does half the job looks exactly like one that does all
  # of it.
  # ============================================================
  # The function itself
  # ============================================================

  # RESTATED 2026-09-21. This said a single pull fetches one batch and returns
  # "there may be more", which pins the driver's batching contract: a driver
  # that fetched everything in one call would fail it while the caller saw the
  # same two copies agree. What a caller can see is that both copies hold the
  # same commits once the pass is over, however many batches that took.
  Scenario: one pass leaves both copies holding the same commits
    Given a local copy and its primary
    When replication runs
    Then the primary holds every commit made locally, and the local copy holds every commit made on the primary
    And a write waiting since the last shutdown is still there, not overwritten by what came down after it

  # RESTATED 2026-09-21. This said it raises. A failure in this package travels
  # by return: every function that can fail says so in its return type, and
  # sending to the cloud is one of them. So the result is a fault coming back,
  # and the line about what the previous version returned is gone, because what
  # some deleted code did is not something a caller can observe.
  Scenario: a local-only database says it cannot replicate
    Given a database with no primary
    When replication is asked for on it
    Then a fault comes back naming the database, and nothing reports success

  # ============================================================
  # Opportunistic — replication yields to real work
  # ============================================================

  # RESTATED 2026-09-21, AS TWO SCENARIOS. The original named a lock as the
  # thing being measured and then argued about which signal to measure it with.
  # A caller never sees a lock. A caller sees whether its own work waited, and
  # the two halves of the claim are two different callers: a writer must never
  # queue behind a round trip to the cloud, and a reader must never hold one
  # off. Both are below, and neither survives replication being made to wait on
  # the wrong thing.
  Scenario: a waiting writer never waits for a round trip to the primary
    Given a replicated database with a write waiting to go out
    When a replication pass is considered
    Then the write lands without waiting for the primary to answer

  Scenario: readers do not hold replication off
    Given a replicated database serving reads and nothing else
    When a replication pass is considered
    Then the pass runs, because no reader ever has to wait for it

  Scenario: a pass that finds a writer waiting defers instead of queueing
    Given a replicated database under sustained write load
    When ten replication passes are considered while a writer waits
    Then none of them run, and when the load clears exactly one pass runs rather than ten, because a deferred pass is dropped and not accumulated

  # DELETED 2026-09-21: "nothing is on a clock". It asked that no timer wakes,
  # which names a timer as the thing being measured, and an implementation that
  # woke on a timer and found nothing to do would be indistinguishable from one
  # that did not. The result underneath it -- a database with nothing pending
  # takes no connection and makes no round trip -- is already a scenario in
  # `tests/bdd/features/lifecycle/lifecycle_catch_up.feature`, so it is not
  # said twice.

  # ============================================================
  # Non-blocking — no caller ever waits for the network
  # ============================================================

  Scenario: no read or write waits for replication
    Given a replicated database serving reads and writes
    When a read and a write run
    Then neither awaits a remote round trip, because a local commit is sub-millisecond and a cloud round trip is not

  Scenario: opening a copy that already exists locally does not wait
    Given a local copy that already exists
    When the database is opened on it
    Then the open does not replicate before returning, because the schema is already on disk and only DATA can be behind — which is the eventual consistency the caller asked for, not a weakening of it

  # RESTATED 2026-09-21. Its own second line said the copying belongs to the
  # engine, and then pinned two of the engine's timings, 2.8 seconds for one
  # cold open and 20 to 25 seconds for 25 at once. A driver release moves both
  # numbers without changing anything a caller asked for. One line is left, and
  # it is the one this library owes: the open does not come back early.
  Scenario: opening a copy that is not on disk yet waits for the whole database
    Given no local copy
    When the database is opened against a primary
    Then the open returns only once the whole database is on disk, because a database with no schema is unusable rather than merely stale

  # ============================================================
  # Shutdown — the durability guarantee, and the one place blocking is correct
  # ============================================================

  Scenario: shutdown is trapped, not left to whoever remembers to call close
    Given a replicated database whose caller asked for shutdown replication
    When the process receives SIGTERM or SIGINT
    Then replication runs to completion before the process exits
    And any handler already installed by the host application still runs, because a library that silently replaces a caller's signal handling breaks the program it is serving

  Scenario: the shutdown policy is stated, never defaulted
    Given a caller that has not asked for shutdown replication
    When a database is opened
    Then no signal handler is installed, because installing one behind a caller's back changes the behavior of a program that never opted into it
    And the shutdown policy is a REQUIRED argument with no default, because a default cannot tell "chose this" from "never knew there was a choice" — and on ephemeral disk a default would silently pick the losing side of the exact failure it exists to prevent

  Scenario: shutdown replication blocks and stops being polite
    Given a shutdown in progress with local commits the primary has not seen
    When shutdown replication runs
    Then it ignores load entirely and takes the connection, because the politeness that is correct during service is data loss during shutdown
    And it blocks until the primary has every local commit, because on ephemeral disk anything not replicated when the process dies is gone

  # DELETED 2026-09-21: "the delay explains itself while it is happening". It
  # pinned how often a line is written, which is a cadence no caller reads and
  # no honest rewrite has to keep. Its surviving half, that every line names the
  # database and the time elapsed, is word for word the scenario "it says what
  # it is doing while it does it" at the top of this same file.
