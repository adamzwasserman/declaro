@bdd
Feature: What a database is when you open it, and what that decides
  Three openers, one value. `open_sqlite`, `open_turso` and `open_postgresql` each
  return the same `Database`, carrying the callables that engine needs. Everything the
  library does afterwards reads that value rather than asking what engine it is.

  One predicate branches the rest of the library: whether the database has a primary.
  A synced replica takes ONE connection, measured 2026-08-12 against a real replica on
  pyturso 0.7.2:

    20 writes, sequential, 1 connection    20 local -> 20 on primary
    8 writes over 8 connections             5 local ->  0 on primary, no convergence
    opening a 2nd sync connection           "database is busy", 3 of 4 runs failed

  So a replicated database serialises its writers and a local one does not, and that
  choice is derived from the primary rather than stored beside it. A second flag could
  disagree with the first; a derived answer cannot. This is declaro-eer and declaro-dna.

  Scenario: opening a local database does not connect to anything
    Given a temporary directory
    When a local SQLite database is opened
    Then the value is returned without a connection being made
    And it reports that it is not replicated

  # A REPLICATED DATABASE IS THE EXCEPTION, DELIBERATELY. It connects at open
  # and HOLDS that connection for every write, because opening one per write is
  # declaro-dna: one sync handshake and one OS worker thread each time, which
  # took a consumer's box down at 20 concurrent signups. The cost is that
  # opening blocks on the network; the request path afterwards does not.
  Scenario: opening a replicated database connects once and keeps it
    Given a temporary directory
    When a Turso database is opened against a primary that is not there
    Then the opener fails rather than returning a database that cannot sync

  Scenario: a shutdown policy must be stated
    Given a temporary directory
    When a local SQLite database is opened without saying what shutdown means
    Then the opener refuses

  # RECORDED, NOT ASSERTED AS DESIRABLE. CLAUDE.md states "`open_sqlite` and
  # `open_postgresql` accept only `exit_immediately`; asking a database with no
  # primary to replicate at shutdown is refused." Measured 2026-08-17: it
  # accepts "replicate", and it accepts "nonsense" too, so it validates
  # nothing. A local database can therefore promise at SIGTERM to push to a
  # primary it does not have. This pins what happens so a fix flips it.
  Scenario: a local database is not yet stopped from promising to replicate
    Given a temporary directory
    When a local SQLite database is opened promising to replicate at shutdown
    Then it is accepted, though the documentation says it is refused

  Scenario: a local database leaves its writers unserialised
    Given a temporary directory
    When a local Turso database is opened
    Then it reports that it is not replicated
    And it carries no write lock, because nothing needs serialising

  Scenario: a replicated database serialises its writers
    Given a temporary directory
    When a Turso database is opened with a primary
    Then it reports that it is replicated
    And it carries a write lock, because the replica takes one connection

  Scenario: the answer comes from the primary, not from a flag beside it
    Given a temporary directory
    When a Turso database is opened with a primary
    Then removing the primary changes the answer
    And nothing else has to be updated to keep them agreeing

  Scenario: a closed database refuses further work rather than reconnecting
    Given a temporary directory
    When a local SQLite database is opened and then closed
    Then asking it to read raises rather than opening a new connection
