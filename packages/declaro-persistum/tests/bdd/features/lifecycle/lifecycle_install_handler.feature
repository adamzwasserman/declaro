@bdd @specification
Feature: lifecycle_install_handler -- One handler for the process
  Installed by the first open that needs one, and installing it twice changes
  nothing. A caller cannot forget to do this, because nobody asks for it.

  Scenario: the first open installs it
    When a replicating database is opened
    Then a signal handler is installed

  Scenario: a second open finds it there
    When a second replicating database is opened
    Then one handler is installed, not two

  Scenario: it chains, it never replaces
    Given a host application with its own handler
    When the handler is installed
    Then the host's handler still runs

  # ADDED 2026-09-22, while building this. Chaining alone is not enough: a host
  # handler is free to end the process, and one that does would take the local
  # commits with it. The order is the requirement, and nothing else in the suite
  # says it.
  Scenario: what the library owes runs before the host's handler is reached
    Given a host application with its own handler
    When the handler is installed
    Then replication is asked for before the host's handler is reached
