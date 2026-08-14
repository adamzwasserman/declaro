Feature: Replication — one function, both directions, out of the way of real work
  A local copy and its cloud primary are brought into conformity. That is replication.
  "Sync" in this package means synchronous and nothing else.

  THE WORD WAS ATTACHED TO HALF THE JOB, ON BOTH BRANCHES.

    replicate(db)   sent local commits UP and never brought anything down
    refresh(db)     brought changes DOWN and never sent anything up
    refresh_once    called pull() ONCE, though pyturso's pull fetches one batch

  The module comment above them said these functions "bring the two copies into
  conformity". Neither did. A caller that only ever called `replicate` had a local copy
  that learned nothing about its primary for as long as the process lived — and nothing
  said so, because a function that does half the job looks exactly like one that does all
  of it.

  # ============================================================
  # The function itself
  # ============================================================

  Scenario: replicate moves data in both directions in one pass
    Given a local copy and its primary
    When replicate runs
    Then local commits the primary has not seen go up first, and only then do the primary's changes come down, so a write waiting since the last shutdown is never overwritten by what follows it
    And it keeps bringing changes down until the primary reports nothing further, because a single pull fetches one batch and returns "there may be more"

  Scenario: a local-only database refuses to replicate rather than pretending
    Given a database with no primary
    When replicate is asked for on a path with no primary
    Then it raises, naming the database, because a local-only database has nothing to bring into conformity
    And the previous version returned True "by vacuity rather than by success", which a caller cannot tell apart from a replication that worked

  # ============================================================
  # Opportunistic — replication yields to real work
  # ============================================================

  Scenario: replication runs only when no writer is waiting
    Given a replicated database whose background replication is running
    When a replication pass is considered
    Then it proceeds only when no writer is waiting for the serialise lock, because replication goes out on the held connection and every waiting writer is a request paying for the round trip
    And load is counted as writers waiting for that lock, NOT as CPU and NOT as active readers — a reader takes no lock at all and never contends with replication
    And it is the exact resource replication takes rather than a cheaper signal standing in for it, which is the substitution that has already cost this package three production incidents

  Scenario: a pass that finds a writer waiting defers instead of queueing
    Given a replicated database under sustained write load
    When ten replication passes are considered while a writer waits
    Then none of them run, and when the load clears exactly one pass runs rather than ten, because a deferred pass is dropped and not accumulated

  Scenario: nothing is on a clock
    Given a replicated database with no pending writes and no waiters
    When time passes
    Then no timer wakes to take the connection and discover there is nothing to do

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

  Scenario: opening a copy that does not exist locally must wait, and that is the engine
    Given no local copy
    When the database is opened against a primary
    Then the whole database is copied before the open returns, because a database with no schema is unusable rather than merely stale
    And that copy is the engine's, not persistum's — measured 2026-08-13 at 2.8s alone and 20-25s when 25 cold opens are issued at once, because they serialize

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
    Then no signal handler is installed, because installing one behind a caller's back changes the behaviour of a program that never opted into it
    And the shutdown policy is a REQUIRED argument with no default, because a default cannot tell "chose this" from "never knew there was a choice" — and on ephemeral disk a default would silently pick the losing side of the exact failure it exists to prevent

  Scenario: shutdown replication blocks and stops being polite
    Given a shutdown in progress with local commits the primary has not seen
    When shutdown replication runs
    Then it ignores load entirely and takes the connection, because the politeness that is correct during service is data loss during shutdown
    And it blocks until the primary has every local commit, because on ephemeral disk anything not replicated when the process dies is gone

  Scenario: the delay explains itself while it is happening
    Given a shutdown that is taking a long time
    When shutdown replication is running
    Then it logs when it starts and keeps logging as it goes, so an operator watching a process refuse to exit can see it is working rather than hung
    And every line names the database and the elapsed time, because "replicating" alone does not distinguish progress from a stall, and does not say WHICH database is holding up the exit

  Scenario: a shutdown cut short says so
    Given a platform grace period shorter than the replication needs
    When the process is killed before replication completes
    Then the last line logged states that the data has not yet reached the primary, because the platform can always cut this short and a silent truncation is indistinguishable from success

  # ============================================================
  # Explicit — a caller asking for replication directly
  # ============================================================

  Scenario: a caller can ask for replication and wait for it
    Given a caller that needs the primary current before it continues
    When it calls flush and awaits it
    Then it returns once both directions have completed, and it ignores load, because a caller that awaited this has already said it is willing to wait

  Scenario: replicating a path creates the local copy when there is not one yet
    Given a path with no local copy and a primary
    When replicate is asked for on that path
    Then the local copy is created and filled from the primary, and the call succeeds
    And this is how a database is provisioned before anyone opens it — the same verb whether or not a copy exists yet, because a caller should not have to know which case it is in to ask for the same outcome
