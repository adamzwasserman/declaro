@bdd @specification
Feature: session_open_connection -- Asking the driver for a connection
  One outward call. It hands back the connection the database reads on, and that
  connection is held and passed onward, never opened up and read.

  WHY NOTHING READS IT. `docs/architecture.md` refuses it in its own words: a
  driver connection does not say which engine it belongs to, and the closest you
  can get is where its class happens to live, which makes the SQL this package
  generates depend on somebody else's file layout.

  Scenario: it hands back a connection
    When the driver is asked for a connection to a local database
    Then a connection comes back

  Scenario: nothing asks the connection what engine it is
    When a connection is opened
    Then the engine came from the target, not from the connection

  Scenario: a database that cannot be reached is a fault, not a handle
    When the driver is asked for a connection to a host that does not exist
    Then a fault comes back naming the target
    And no handle is returned

  # MOVED HERE 2026-09-21 from `permitted_connections.feature`. That function
  # settles a number and runs nothing: it reads no clock, opens nothing, and
  # neither its arguments nor its answer can carry a connection. Whether a read
  # waits, whether five overlap, and whether reads queue behind a sync are all
  # answered by what this boundary opens, so they are asked here.

  Scenario: a read does not wait for a writer
    Given a write in flight
    When a caller reads
    Then the read answers without waiting for the write to finish

  Scenario: reads run beside each other
    When five callers read at once, each taking time
    Then the five overlap rather than running one after another

  Scenario: reads do not queue behind the sync connection
    Given a replicated database
    When many callers read and write at once
    Then the reads did not queue behind it
