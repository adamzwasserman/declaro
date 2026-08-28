@bdd
Feature: Agreeing to overwrite a snapshot that is already there
  As someone taking a snapshot over one that exists
  I want to be asked, if I am there to answer
  So that an automated run does not stop on a question nobody can hear

  # NO TERMINAL MEANS NO PROMPT. `input()` raises EOFError on a pipe, in CI, in
  # cron and in a container, and taking a snapshot after a deploy is exactly
  # the automated case. So the run is refused with a message naming --force,
  # rather than dying on an unanswerable question.
  #
  # WHETHER THERE IS A TERMINAL IS READ AT THE EDGE. The dispatcher used to
  # call sys.stdin.isatty() itself while declaring it did no I/O. It takes the
  # answer now, so a scenario can put both cases to it without a terminal.

  @precommit
  Scenario: with somebody at the terminal, the question is put to them
    Given a snapshot already exists
    When the overwrite is considered with somebody at the terminal
    Then they are asked

  @precommit
  Scenario: with no terminal, the run is refused rather than asked
    Given a snapshot already exists
    When the overwrite is considered with no terminal
    Then nobody is asked
    And the exit code is 1

  @precommit
  Scenario: with no snapshot there, nothing is asked and the work runs
    Given no snapshot exists
    When the overwrite is considered with no terminal
    Then nobody is asked
    And the work ran
