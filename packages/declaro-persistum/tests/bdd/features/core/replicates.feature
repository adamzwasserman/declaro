@bdd @specification
Feature: replicates -- Whether this database replicates at all
  Derived from the channel rather than stored beside it, because a second flag
  could disagree with the first. It decides whether any sync card is reached.

  # IT WAS DERIVED FROM A `primary` UNTIL 2026-09-24, and `Database` has not had
  # one since the crew was rebuilt: a target and a primary were two names for
  # where a database is, and what this question turns on is whether there is a
  # line to one. That line is the channel, which is the thing there is exactly
  # one of, and it is either open or it is the named absence.

  Scenario: a database holding a channel replicates
    Given a database opened with a channel to a primary
    Then it replicates

  Scenario: a local database does not
    Given a database opened with no channel
    Then it does not replicate

  Scenario: nothing else records the answer
    Then no field anywhere says it, beside the channel itself
