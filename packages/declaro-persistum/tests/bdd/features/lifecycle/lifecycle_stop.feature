@bdd @specification
Feature: lifecycle_stop -- The platform says stop
  The signal arrives with nothing in its hands. What it has to work through is
  the list of databases that asked to replicate at exit, and the module reads
  that for itself.

  Scenario: the handler runs replication to completion before the process exits
    Given a replicated database whose caller asked for shutdown replication
    When the process receives SIGTERM
    Then replication finishes before the process exits

  Scenario: a handler the host already installed still runs
    Given a host application with its own signal handler
    When the process receives SIGTERM
    Then the host's handler ran too

  Scenario: nothing is installed for a caller who did not ask
    Given a database opened without asking for shutdown replication
    When the process starts
    Then no signal handler was installed
