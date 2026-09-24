@bdd @specification
Feature: lifecycle_stop -- The platform says stop
  The signal arrives with nothing in its hands. What it has to work through is
  the list of databases that asked to replicate at exit, and shutdown reads that
  for itself.

  # TWO SCENARIOS MOVED OUT ON 2026-09-24, to the card that owns them. Whether a
  # host's own handler still runs, and whether anything is installed for a caller
  # who never asked, are both about installing the handler rather than about
  # what happens when it fires. `lifecycle_install_handler.feature` says both.

  Scenario: the signal runs shutdown to completion
    Given a database that asked to replicate at exit
    When the platform says stop
    Then its local commits went up and its connections were closed

  Scenario: it hands over nothing, because the signal carries nothing
    Given two databases that asked to replicate at exit
    When the platform says stop
    Then both were shut down, though the signal named neither

  # A DOOR NEVER RAISES, AND THIS ONE RUNS INSIDE A SIGNAL HANDLER. An exception
  # here reaches the platform mid-signal, where there is no caller to catch it
  # and the process is already on its way out: whatever it would have said goes
  # nowhere, and the remaining databases are never reached.
  Scenario: a channel that will not take it is reported
    Given a database whose channel refuses every attempt
    When the platform says stop
    Then a fault comes back and nothing was raised

  # THE REGISTER CAN HOLD SOMETHING THAT IS NOT A DATABASE, and that is the case
  # this door has to survive: the scenario above never raises at all, because
  # every driver failure is already caught and turned into a value long before
  # it gets here. Something malformed on the register is the real path, and
  # without it "nothing was raised" is a sentence nothing exercises.
  Scenario: something malformed on the register does not raise out to the platform
    Given a register holding something that is not a database
    When the platform says stop
    Then a fault comes back and nothing was raised
