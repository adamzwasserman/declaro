@bdd @specification
Feature: lifecycle_read_registered -- Reading the list the signal has to work through
  The signal carries no arguments. Without this the door would have to be handed
  a list the module wrote itself.

  Scenario: every registered database comes back
    Given three databases opened asking to replicate at exit
    When the list is read
    Then all three come back

  Scenario: nothing registered is an answer
    Given no database asking to replicate
    When the list is read
    Then it is empty, and that is not a failure
