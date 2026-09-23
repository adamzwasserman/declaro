@bdd @specification
Feature: crew_make -- A crew with nobody in it yet
  A crew holds the writes that have arrived, the caller waiting on each one, the
  bell that wakes a worker, the workers themselves and the switch that stops
  them. All of it is empty whenever nobody is mid-flight, which is most of the
  time. Nothing about it survives the process, because every write that matters
  has a caller holding a ticket for it.

  Scenario: a new crew holds nothing
    When a crew is made
    Then nothing is waiting and nobody is working

  Scenario: it can be slept on and rung
    When a crew is made
    Then a worker can sleep on its bell and a deposit can ring it
